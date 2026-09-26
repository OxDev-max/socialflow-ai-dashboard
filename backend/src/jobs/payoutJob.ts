import { Job } from 'bullmq';
import { queueManager } from '../queues/queueManager';
import { PayoutJobData, PAYOUT_QUEUE_NAME } from '../queues/payoutQueue';
import { prisma } from '../lib/prisma';
import { createHash } from 'crypto';
import { LockService } from '../utils/LockService';
import { createLogger } from '../lib/logger';
import {
  payoutJobAttemptedTotal,
  payoutJobSucceededTotal,
  payoutJobFailedTotal,
  payoutJobDurationSeconds,
} from '../lib/metrics';

const logger = createLogger('payout-job');

/**
 * Derive a deterministic transaction hash for a payout.
 * In production this would be the actual Stellar transaction hash
 * returned by the Horizon API after building and submitting the tx.
 * By computing it deterministically before submission we can detect
 * duplicate retries and avoid paying a user twice.
 */
function deriveTransactionHash(data: {
  groupId: string;
  amount: number;
  recipient: string;
  currency: string;
  jobId: string;
}): string {
  const payload = `${data.jobId}|${data.groupId}|${data.recipient}|${data.amount}|${data.currency}`;
  return `stellar-tx-${createHash('sha256').update(payload).digest('hex')}`;
}

/**
 * Payout job processor
 * Handles processing payouts with retry logic and error handling
 *
 * Idempotency for Stellar/crypto transactions:
 * Before submitting a Stellar transaction the hash is stored in the
 * PayoutTransaction table. On retry the job checks whether the hash
 * already exists and skips submission, preventing duplicate payouts.
 */
export async function processPayoutJob(job: Job<PayoutJobData>) {
  const {
    groupId,
    amount,
    recipient,
    recipientType,
    currency,
    description: _description,
    metadata,
  } = job.data;

  logger.info(`Processing job ${job.id}`, { jobId: job.id, groupId, amount, currency, recipient });

  payoutJobAttemptedTotal.inc();
  const endTimer = payoutJobDurationSeconds.startTimer();

  try {
    // Log job progress
    await job.updateProgress(10);

    // Validate payout data
    if (
      !groupId ||
      amount === undefined ||
      amount === null ||
      !recipient ||
      !recipientType ||
      !currency
    ) {
      throw new Error('Missing required payout fields');
    }

    if (amount <= 0) {
      throw new Error('Payout amount must be greater than 0');
    }

    await job.updateProgress(20);

    // ── Row-level lock ─────────────────────────────────────────────────────
    // Acquire an exclusive distributed lock keyed to the payout group/job so
    // that concurrent triggers (e.g. manual retry + scheduled run) cannot
    // both read a pending record and initiate duplicate transfers.
    const result = await LockService.withLock(`payout:${groupId}:${job.id ?? 'unknown'}`, async () => {
      // ── Stellar / crypto idempotency check ──────────────────────────────
      let transactionHash: string | undefined;
      let skipped = false;

      if (recipientType === 'crypto' || recipientType === 'wallet') {
        transactionHash = deriveTransactionHash({
          groupId,
          amount,
          recipient,
          currency,
          jobId: job.id ?? 'unknown',
        });

        const existing = await prisma.payoutTransaction.findUnique({
          where: { transactionHash },
        });

        if (existing) {
          logger.info(`Duplicate detected for job ${job.id} — skipping submission`, {
            jobId: job.id,
            existingTransactionId: existing.id,
          });
          skipped = true;
        } else {
          await prisma.payoutTransaction.create({
            data: {
              groupId,
              recipient,
              amount,
              currency,
              transactionHash,
              jobId: job.id ?? 'unknown',
              status: 'pending',
            },
          });
        }
      }

      // ── Payment processing ───────────────────────────────────────────────
      if (!skipped) {
        await new Promise((resolve) => setTimeout(resolve, 500));
        await job.updateProgress(80);

        if (recipientType === 'crypto' || recipientType === 'wallet') {
          await new Promise((resolve) => setTimeout(resolve, 200));

          if (transactionHash) {
            await prisma.payoutTransaction.update({
              where: { transactionHash },
              data: { status: 'confirmed', confirmedAt: new Date() },
            });
          }
        }
      }

      await job.updateProgress(95);

      logger.info(`Job ${job.id} completed successfully`, {
        jobId: job.id,
        amount,
        currency,
        recipient,
        skipped,
      });

      return {
        success: true,
        transactionId: job.id,
        transactionHash,
        groupId,
        amount,
        currency,
        recipient,
        recipientType,
        status: 'completed',
        skipped,
        processedAt: new Date().toISOString(),
        metadata,
      };
    });

    payoutJobSucceededTotal.inc();
    endTimer();

    return result;
  } catch (error: any) {
    const reason = error.message as string;
    logger.error(`Job ${job.id} failed`, { jobId: job.id, reason });

    payoutJobFailedTotal.inc();
    endTimer();

    try {
      await prisma.payoutFailure.create({
        data: {
          jobId: job.id ?? 'unknown',
          groupId: groupId ?? 'unknown',
          recipient: recipient ?? 'unknown',
          amount: amount ?? 0,
          currency: currency ?? 'unknown',
          reason,
        },
      });
    } catch (dbErr: any) {
      logger.error('Failed to persist payout failure record', {
        jobId: job.id,
        error: dbErr.message,
      });
    }

    throw new Error(`Failed to process payout: ${reason}`);
  }
}

/**
 * Create payout worker using the queue manager
 */
export function createPayoutWorker() {
  return queueManager.createWorker('payout', processPayoutJob, {
    concurrency: 3, // Lower concurrency for financial transactions
  });
}

/**
 * Process batch payout job
 */
export async function processBatchPayoutJob(job: Job<{ payouts: PayoutJobData[] }>) {
  const { payouts } = job.data;

  logger.info(`Processing batch job ${job.id}`, { jobId: job.id, payoutCount: payouts.length });

  const results: Array<{
    success: boolean;
    transactionId?: string;
    recipient?: string;
    error?: string;
  }> = [];

  let totalAmount = 0;
  let successfulAmount = 0;

  for (let i = 0; i < payouts.length; i++) {
    const payout = payouts[i];

    try {
      // Validate payout
      if (!payout.groupId || !payout.amount || !payout.recipient) {
        throw new Error('Missing required payout fields');
      }

      totalAmount += payout.amount;

      // Simulate processing
      await new Promise((resolve) => setTimeout(resolve, 300));

      successfulAmount += payout.amount;

      results.push({
        success: true,
        transactionId: `${job.id}-${i}`,
        recipient: payout.recipient,
      });

      await job.updateProgress(Math.floor(((i + 1) / payouts.length) * 100));
    } catch (error: any) {
      results.push({
        success: false,
        recipient: payout.recipient,
        error: error.message,
      });
    }
  }

  const successful = results.filter((r) => r.success).length;
  const failed = results.filter((r) => !r.success).length;

  logger.info(`Batch job ${job.id} completed`, {
    jobId: job.id,
    successful,
    successfulAmount,
    failed,
  });

  // Re-enqueue failed items individually so each gets its own retry budget
  if (failed > 0) {
    const failedPayouts = payouts.filter((_, i) => !results[i].success);
    const reEnqueueJobs = failedPayouts.map((payout) => ({
      name: 'process-payout',
      data: payout,
      options: { priority: 1 },
    }));
    await queueManager.addBulkJobs(PAYOUT_QUEUE_NAME, reEnqueueJobs);

    throw new Error(
      `Batch job ${job.id} had ${failed}/${payouts.length} failed payouts; re-enqueued for retry`,
    );
  }

  return {
    success: true,
    jobId: job.id,
    totalPayouts: payouts.length,
    successfulPayouts: successful,
    failedPayouts: failed,
    totalAmount,
    successfulAmount,
    results,
    completedAt: new Date().toISOString(),
  };
}
