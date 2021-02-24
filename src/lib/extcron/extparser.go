package extcron

import (
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

type ExtParser struct {
	parser cron.Parser
}

var standaloneParser = NewParser()

func NewParser() cron.ScheduleParser {
	return ExtParser{cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)}
}

func (p ExtParser) Parser(spec string) (cron.Schedule, error) {
	if spec == "@manually" {
		return At(time.Time{}), nil
	}

	const at = "@at "
	if strings.HasPrefix(spec, at) {
		data, err := time.Parse(time.RFC3339, spec[len(at):])
		if err != nil {
			return nil, fmt.Errorf("fail to parse date %s: %s", spec, err)
		}
		return At(data), nil
	}
	return p.parser.Parse(spec)
}

func Parse(spec string) (cron.Schedule, error) {
	return standaloneParser.Parse(spec)
}
