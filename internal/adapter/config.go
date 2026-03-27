package adapter

import (
	"fmt"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// Config holds toc-adapter sidecar configuration.
type Config struct {
	NATS             NATSConfig     `koanf:"nats"`
	PipelineID       string         `koanf:"pipeline_id"`
	PollInterval     time.Duration  `koanf:"poll_interval"`
	StalenessWindows int            `koanf:"staleness_windows"`
	Sources          []SourceConfig `koanf:"sources"`
}

// NATSConfig holds NATS connection settings.
type NATSConfig struct {
	URL    string `koanf:"url"`
	Prefix string `koanf:"prefix"`
}

// SourceConfig describes one external data source to poll.
type SourceConfig struct {
	Type    string           `koanf:"type"`
	URL     string           `koanf:"url"`
	Timeout time.Duration    `koanf:"timeout"`
	Stages  []StageExtraction `koanf:"stages"`
}

// StageExtraction describes how to extract metrics for one stage from
// a source's response.
type StageExtraction struct {
	Name     string                 `koanf:"name"`
	Selector string                 `koanf:"selector"`
	Fields   map[string]FieldConfig `koanf:"fields"`
}

// FieldConfig describes how to extract a single metric field from a
// JSON response.
type FieldConfig struct {
	Path       string  `koanf:"path"`
	Required   bool    `koanf:"required"`
	Multiplier float64 `koanf:"multiplier"`
}

// LoadConfig reads configuration from a YAML file and overlays
// environment variables with the TOC_ADAPTER_ prefix.
func LoadConfig(path string) (Config, error) {
	k := koanf.New(".")

	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return Config{}, fmt.Errorf("load config file: %w", err)
	}

	// Environment variables override file values.
	// Single underscore prefix, double underscore for nesting.
	// TOC_ADAPTER_PIPELINE_ID -> pipeline_id (flat key)
	// TOC_ADAPTER_NATS__URL   -> nats.url    (nested via __)
	if err := k.Load(env.Provider("TOC_ADAPTER_", ".", func(s string) string {
		s = strings.TrimPrefix(s, "TOC_ADAPTER_")
		s = strings.ToLower(s)
		s = strings.ReplaceAll(s, "__", ".")
		return s
	}), nil); err != nil {
		return Config{}, fmt.Errorf("load env config: %w", err)
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal config: %w", err)
	}

	// Apply defaults.
	if cfg.NATS.Prefix == "" {
		cfg.NATS.Prefix = "toc"
	}
	if cfg.StalenessWindows == 0 {
		cfg.StalenessWindows = 5
	}
	for i := range cfg.Sources {
		if cfg.Sources[i].Timeout == 0 {
			cfg.Sources[i].Timeout = 5 * time.Second
		}
		// Default multiplier to 1.0 during normalization so 0 is not
		// overloaded as "unset" at extraction time.
		for j := range cfg.Sources[i].Stages {
			for k, fc := range cfg.Sources[i].Stages[j].Fields {
				if fc.Multiplier == 0 {
					fc.Multiplier = 1.0
					cfg.Sources[i].Stages[j].Fields[k] = fc
				}
			}
		}
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// validSourceTypes lists the supported source types.
var validSourceTypes = map[string]bool{"rest": true}

// validFieldNames lists the recognized StageMetrics field names.
var validFieldNames = map[string]bool{
	"completions": true, "failures": true, "arrivals": true,
	"queue_depth": true, "workers": true,
	"busy_ns": true, "idle_ns": true, "blocked_ns": true,
}

// Validate checks that the configuration is well-formed.
func (c Config) Validate() error {
	if c.PipelineID == "" {
		return fmt.Errorf("config: pipeline_id is required")
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("config: poll_interval must be > 0")
	}
	if len(c.Sources) == 0 {
		return fmt.Errorf("config: at least one source is required")
	}

	stageNames := make(map[string]bool)
	for i, src := range c.Sources {
		if !validSourceTypes[src.Type] {
			return fmt.Errorf("config: source[%d] has unknown type %q", i, src.Type)
		}
		if len(src.Stages) == 0 {
			return fmt.Errorf("config: source[%d] must have at least one stage", i)
		}
		for j, stage := range src.Stages {
			if stage.Name == "" {
				return fmt.Errorf("config: source[%d].stages[%d] must have a name", i, j)
			}
			if len(stage.Fields) == 0 {
				return fmt.Errorf("config: source[%d].stages[%d] (%s) must have at least one field", i, j, stage.Name)
			}
			for fieldName := range stage.Fields {
				if !validFieldNames[fieldName] {
					return fmt.Errorf("config: source[%d].stages[%d] (%s) has unknown field %q", i, j, stage.Name, fieldName)
				}
			}
			if stageNames[stage.Name] {
				return fmt.Errorf("config: duplicate stage name %q", stage.Name)
			}
			stageNames[stage.Name] = true
		}
	}

	return nil
}
