package config

import (
	"os"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	sqlite "github.com/glebarez/sqlite"
)

type Config struct {
	Port     string
	DBDriver string
	DBDSN    string
}

func Load() Config {
	return Config{
		Port:     env("PORT", "8080"),
		DBDriver: env("DB_DRIVER", "sqlite"),
		DBDSN:    env("DB_DSN", "webhook_relay.db"),
	}
}

func (c Config) OpenDB() (*gorm.DB, error) {
	switch c.DBDriver {
	case "postgres":
		return gorm.Open(postgres.Open(c.DBDSN), &gorm.Config{})
	default:
		return gorm.Open(sqlite.Open(c.DBDSN), &gorm.Config{})
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
