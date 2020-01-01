package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	mOperationStatus = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sftp_operation_status",
		Help: "Represents SFTP operation statuses",
	},
		[]string{"method", "status"},
	)
	mAWSSessionError = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sftp_aws_session_error",
		Help: "The total number of session errors",
	},
	)
	mPermissionsError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sftp_permissions_error",
		Help: "The total number of permission errors",
	},
		[]string{"method"},
	)
	mUsersConnected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sftp_users_connected",
		Help: "The number of users connected now",
	},
	)
	mMemoryBufferPoolMax = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sftp_memory_buffer_pool_max",
		Help: "The number of maximum memory buffers in the pool",
	},
	)
	mMemoryBufferPoolUsed = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sftp_memory_buffer_pool_used",
		Help: "The number of memory buffers used in the pool",
	},
	)
	mMemoryBufferPoolTimeouts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sftp_memory_buffer_pool_timeouts",
		Help: "The total number of timeouts produced in the pool",
	},
	)
	mReadsBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sftp_reads_bytes_total",
		Help: "The total number of bytes read",
	},
	)
	mWritesBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sftp_writes_bytes_total",
		Help: "The total number of bytes written",
	},
	)
)
