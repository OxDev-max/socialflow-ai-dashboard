variable "env" {
  description = "Environment name (dev or prod)"
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.env)
    error_message = "Environment must be either 'dev' or 'prod'."
  }
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "vpc_id" {
  description = "ID of the VPC where the RDS instance and its security group are created"
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs used for the RDS subnet group"
  type        = list(string)
}

variable "db_name" {
  description = "Name of the initial database created on the RDS instance"
  type        = string
}

variable "db_username" {
  description = "Master username for the RDS instance"
  type        = string
}

variable "db_password" {
  description = "Master password for the RDS instance; supply via a secret store, never commit it"
  type        = string
  sensitive   = true
}

variable "instance_class" {
  description = "RDS instance class (e.g. db.t3.micro for dev, db.t3.medium or larger for prod)"
  type        = string
}

variable "allocated_storage" {
  description = "Allocated storage for the RDS instance in gigabytes"
  type        = number
}

variable "engine_version" {
  description = "PostgreSQL engine version for the RDS instance (e.g. 15.4)"
  type        = string
}

variable "backup_retention_period" {
  description = "Number of days to retain automated backups; 0 disables backups"
  type        = number
}

variable "multi_az" {
  description = "Whether to deploy the RDS instance across multiple Availability Zones for high availability"
  type        = bool
}
