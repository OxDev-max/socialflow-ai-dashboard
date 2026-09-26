import { createLogger } from '../lib/logger';
import {
  videoTranscodeJobsTotal,
  videoTranscodeJobDurationSeconds,
} from '../lib/metrics';

const logger = createLogger('VideoQueue');

interface QueueJob {
  id: string;
  priority: number;
  addedAt: Date;
  startedAt?: Date;
  processor: () => Promise<void>;
  resolve: () => void;
  reject: (err: unknown) => void;
}

/**
 * High CPU worker queue for video transcoding
 * Processes jobs sequentially to avoid overwhelming the CPU
 */
class VideoQueue {
  private queue: QueueJob[] = [];
  private processing = false;
  private maxConcurrent = 1; // Process one video at a time for CPU-intensive tasks
  private activeJobs = 0;
  private pendingMaxConcurrent: number | null = null;

  /**
   * Add a job to the queue.
   * Returns a promise that resolves (or rejects) when the job finishes.
   */
  public addJob(
    jobId: string,
    processor: () => Promise<void>,
    priority: number = 0,
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const queueJob: QueueJob = {
        id: jobId,
        priority,
        addedAt: new Date(),
        processor,
        resolve,
        reject,
      };

      // Insert job based on priority (higher priority first)
      const insertIndex = this.queue.findIndex((job) => job.priority < priority);
      if (insertIndex === -1) {
        this.queue.push(queueJob);
      } else {
        this.queue.splice(insertIndex, 0, queueJob);
      }

      logger.info(`Job ${jobId} added to queue`, { priority });

      // Start processing if not already running
      if (!this.processing) {
        this.processQueue();
      }
    });
  }

  /**
   * Process jobs in the queue
   */
  private async processQueue(): Promise<void> {
    if (this.processing) {
      return;
    }

    this.processing = true;

    while (this.queue.length > 0 && this.activeJobs < this.maxConcurrent) {
      const job = this.queue.shift();
      if (!job) {
        break;
      }

      this.activeJobs++;
      job.startedAt = new Date();

      logger.info(`Starting job ${job.id}`, { queueLength: this.queue.length });

      const startTime = process.hrtime.bigint();
      let status = 'failed';

      try {
        await job.processor();
        status = 'succeeded';
        logger.info(`Job ${job.id} completed successfully`);
        job.resolve();
      } catch (error) {
        logger.error(`Job ${job.id} failed`, { error });
        job.reject(error);
      } finally {
        const durationSeconds = Number(process.hrtime.bigint() - startTime) / 1e9;
        videoTranscodeJobsTotal.inc({ status });
        videoTranscodeJobDurationSeconds.observe({ status }, durationSeconds);

        this.activeJobs--;
        if (this.activeJobs === 0 && this.pendingMaxConcurrent !== null) {
          this.maxConcurrent = this.pendingMaxConcurrent;
          this.pendingMaxConcurrent = null;
          logger.info(`Applied pending maxConcurrent: ${this.maxConcurrent}`);
        }
      }
    }

    this.processing = false;

    // Continue processing if there are more jobs
    if (this.queue.length > 0) {
      this.processQueue();
    }
  }

  /**
   * Get queue status
   */
  public getStatus() {
    return {
      queueLength: this.queue.length,
      activeJobs: this.activeJobs,
      maxConcurrent: this.maxConcurrent,
      jobs: this.queue.map((job) => ({
        id: job.id,
        priority: job.priority,
        addedAt: job.addedAt,
      })),
    };
  }

  /**
   * Set max concurrent jobs.
   * If jobs are currently active, the new limit is deferred until they drain.
   */
  public setMaxConcurrent(max: number): void {
    const clamped = Math.max(1, max);
    if (this.activeJobs === 0) {
      this.maxConcurrent = clamped;
      this.pendingMaxConcurrent = null;
    } else {
      this.pendingMaxConcurrent = clamped;
      logger.info(`Deferring maxConcurrent change to ${clamped} until active jobs drain`);
    }
  }
}

export const videoQueue = new VideoQueue();
