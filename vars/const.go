package vars

import "time"

var (
	ConnModes = []string{"primary", "secondaryPreferred", "secondary", "nearest", "standalone"}

	ConnectModePrimary            = "primary"
	ConnectModeSecondaryPreferred = "secondaryPreferred"
	ConnectModeSecondary          = "secondary"
	ConnectModeNearset            = "nearest"
	ConnectModeStandalone         = "standalone"
)

const (
	BarLength   = 24
	BarWaitTime = 3 * time.Second
)
