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
	mMemoryPoolsMax = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sftp_memory_pools_max",
		Help: "The number of maximum memory pools",
	},
	)
	mMemoryPoolsUsed = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sftp_memory_pools_used",
		Help: "The number of memory pools used",
	},
	)
)
