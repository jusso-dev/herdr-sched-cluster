package placement

import "encoding/json"

// ReadReport interprets `nighthawk report` JSON. Any error or a health
// other than ok is down. Mode is attended or unattended when health is ok.
func ReadReport(body []byte, err error) (health, mode string) {
	if err != nil {
		return "down", ""
	}
	var r struct {
		Health string `json:"health"`
		Mode   string `json:"mode"`
	}
	if json.Unmarshal(body, &r) != nil || r.Health != "ok" {
		return "down", ""
	}
	return "ok", r.Mode
}
