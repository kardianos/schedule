package main

import (
	`bitbucket.org/kardianos/service/config`
	serv `bitbucket.org/kardianos/service/stdservice`
	`github.com/kardianos/cron`

	`errors`
	`fmt`
	"io/ioutil"
	`net/http`
	"os"
	"os/exec"
	"sync"
)

type UnknownDoErr struct {
	Do string
}

func (err *UnknownDoErr) Error() string {
	return `unknown "Do": ` + err.Do
}

var sch = cron.New()
var conf *config.WatchConfig

type ConfigLine struct {
	At   string
	Do   string
	Args []string

	runError   func(cl *ConfigLine, err error)
	isErrorJob bool

	sync.Mutex
	running bool
}

type AppConfig struct {
	UTC   bool
	Tasks []*ConfigLine
}

func (cl *ConfigLine) Verify() error {
	// Verify arguments.
	switch cl.Do {
	case "ping":
		if len(cl.Args) != 2 {
			return errors.New(`"ping" command must have two arguments: Url, and expected result.`)
		}
	case "exec":
		if len(cl.Args) == 0 {
			return errors.New(`"exec" command must have at least one Args: CMD [ARGS].`)
		}
	default:
		return &UnknownDoErr{Do: cl.Do}
	}
	return nil
}

func (cl *ConfigLine) Running() bool {
	cl.Lock()
	rtest := cl.running
	cl.Unlock()

	return rtest
}

var alreadyRunningError = errors.New("Task already running.")

func (cl *ConfigLine) Run() {
	if cl.Running() {
		cl.runError(cl, alreadyRunningError)
		return
	}
	cl.Lock()
	cl.running = true
	cl.Unlock()
	defer func() {
		cl.Lock()
		cl.running = false
		cl.Unlock()
	}()
	switch cl.Do {
	case "ping":
		res, err := http.Get(cl.Args[0])
		if err != nil {
			cl.runError(cl, err)
			return
		}
		defer res.Body.Close()
		responseBytes, err := ioutil.ReadAll(res.Body)
		if err != nil {
			cl.runError(cl, err)
			return
		}
		responseString := string(responseBytes)
		if responseString != cl.Args[1] {
			cl.runError(cl, fmt.Errorf(`Expected "%s", got: "%s".`, cl.Args[1], responseString))
			return
		}
	case "exec":
		cmd := exec.Command(cl.Args[0], cl.Args[1:]...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			cl.runError(cl, fmt.Errorf("Failed to run command result. Error: %s, output: %s", err, output))
			return
		}
	default:
		panic("unreachable")
	}
}

func (cl *ConfigLine) String() string {
	return fmt.Sprintf("%s @ %s <%v>", cl.Do, cl.At, cl.Args)
}

var SampleConfig = &AppConfig{
	UTC: false,
	Tasks: []*ConfigLine{
		&ConfigLine{
			At: "@error",
			Do: "ping",
			Args: []string{
				"http://hitthisurl.com/error",
				"OK",
			},
		}, &ConfigLine{
			At: "0 5 * * * *",
			Do: "ping",
			Args: []string{
				"http://hitthisurl.com/here",
				"OK",
			},
		}, &ConfigLine{
			At: "@every 1h30m",
			Do: "ping",
			Args: []string{
				"http://hitthisurl.com/here",
				"OK",
			},
		},
	},
}

func main() {
	var schConfig = &AppConfig{}

	sch = cron.New()
	ser := &serv.Config{
		Name:            `Scheduler`,
		DisplayName:     `Scheduler`,
		LongDescription: `Task Scheduler`,

		Start: func(c *serv.Config) {
			log := c.Logger()

			errorJobs := []cron.Job{}
			onRunError := func(cl *ConfigLine, err error) {
				log.Warning("The job \"%s\" failed to run:\n%v", cl.String(), err)
				if cl.isErrorJob {
					return
				}
				for _, ej := range errorJobs {
					ej.Run()
				}
			}

			sch.Start()
			go conf.TriggerC()
		work:
			for {
				select {
				case <-conf.C:
					err := conf.Load(&schConfig)
					if err != nil {
						log.Warning("Failed to load config: %v", err)
						continue work
					}

					// Verify config before stopping.
					for _, sc := range schConfig.Tasks {
						err = sc.Verify()
						if sc.At != "@error" {
							_, err := cron.Parse(sc.At)
							if err != nil {
								log.Warning("Bad schedule <%s>: %v", sc.At, err)
								continue work
							}
						}
						if err != nil {
							log.Warning("Bad task configuration: %v", err)
							continue work
						}
						sc.runError = onRunError
					}

					sch.Stop()
					sch = cron.New()
					sch.UTC = true

					errorJobs = []cron.Job{}
					for _, sc := range schConfig.Tasks {
						if sc.At == "@error" {
							sc.isErrorJob = true
							errorJobs = append(errorJobs, sc)
						} else {
							err = sch.AddJob(sc.At, sc)
							if err != nil {
								// This should not happen as the At is parsed above.
								log.Error("Error from adding the job: %s", sc.String())
								os.Exit(1)
							}
						}
					}
					sch.Start()
					log.Info("v1.1: Configuration loaded.")
				}
			}
		},
		Stop: func(c *serv.Config) {
			sch.Stop()
		},
		Init: func(c *serv.Config) error {
			log := c.Logger()

			var err error

			configFilePath, err := config.GetConfigFilePath("", "")
			if err != nil {
				log.Error("Could not get config file path: %v", err)
				return err
			}

			conf, err = config.NewWatchConfig(configFilePath, config.DecodeJsonConfig, SampleConfig, config.EncodeJsonConfig)
			if err != nil {
				log.Error("Could not open config file path: %v", err)
				return err
			}

			return nil
		},
	}

	ser.Run()
}
