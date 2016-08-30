package beater

import (
	"fmt"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	"github.com/consulthys/retsbeat/config"
	"github.com/jpfielding/gorets/rets"
)
import (
	rets_common "github.com/jpfielding/gorets/cmds/common"
	"errors"
)

type Retsbeat struct {
	done   chan struct{}
	config config.Config
	client publisher.Client

	sessions	[]RetsSession
}

type RetsSession struct {
	Code string
	Config rets_common.Config
	Session rets.Requester
	Resources []MlsResource
}

// Creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	config := config.DefaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Retsbeat{
		done: make(chan struct{}),
		config: config,
	}

	// initialize all RETS sessions
	bt.sessions = make([]RetsSession, len(config.Servers))
	for s := 0; s < len(config.Servers); s++ {
		server := config.Servers[s]
		retsConfig := rets_common.Config{
			URL: server.Connection.URL,
			Username: server.Connection.Username,
			Password: server.Connection.Password,
			UserAgent: server.Connection.UserAgent,
			UserAgentPw: server.Connection.UserAgentPassword,
			Version: server.Connection.RetsVersion,
			WireLog: fmt.Sprintf("/tmp/gorets/%v.log", server.Code),
		}
		session, err := retsConfig.Initialize()
		if err != nil {
			logp.Err("Invalid RETS configuration for %v: %v", server.Code, err)
			continue
		}

		bt.sessions[s] = RetsSession{Code: server.Code, Session: session, Config: retsConfig}

		resources, err := bt.GetMetadataResources(&bt.sessions[s], server.Status, server.Type, server.Custom)
		if err != nil {
			logp.Err("Could not fetch metadata for %v: %v", server.Code, err)
			continue
		}
		bt.sessions[s].Resources = resources
	}

	if len(bt.sessions) == 0 {
		return nil, errors.New("No RETS servers configured")
	}

	logp.Debug("retsbeat", "Init retsbeat")
	logp.Debug("retsbeat", "Period %v\n", bt.config.Period)
	for s := 0; s < len(bt.sessions); s++ {
		logp.Debug("retsbeat", "Will gather resource counts from %v RETS server at %v", bt.sessions[s].Code, bt.sessions[s].Config.URL)
	}

	return bt, nil
}

func (bt *Retsbeat) Run(b *beat.Beat) error {
	logp.Info("retsbeat is running! Hit CTRL-C to stop it.")

	bt.client = b.Publisher.Connect()

	for _, sess := range bt.sessions {
		go func(rs RetsSession) {
			ticker := time.NewTicker(bt.config.Period)
			counter := 1
			for {
				select {
				case <-bt.done:
					goto GotoFinish
				case <-ticker.C:
				}

				timerStart := time.Now()

				logp.Debug("retsbeat", "Resource stats for MLS: %v", rs.Code)
				stats, err := bt.GetResourceStats(&rs)

				if err != nil {
					logp.Err("Error reading MLS stats: %v", err)
				} else {
					event := common.MapStr{
						"@timestamp":   common.Time(time.Now()),
						"type":         "stats",
						"counter":      counter,
						"stats":        stats,
					}

					bt.client.PublishEvent(event)
					logp.Info("RETS stats for %v sent", rs.Code)
					counter++
				}

				timerEnd := time.Now()
				duration := timerEnd.Sub(timerStart)
				if duration.Nanoseconds() > bt.config.Period.Nanoseconds() {
					logp.Warn("Ignoring tick(s) due to processing taking longer than one period")
				}
			}
		GotoFinish:
		}(sess)
	}

	<-bt.done
	return nil
}

func (bt *Retsbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}
