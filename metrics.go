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
)
