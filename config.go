package metaventus_gorm_adapters

import (
	"fmt"
	"os"
)

// GormConfig config nécessaire à la connexion à postgresql
type GormConfig struct {
	DSN           string
	INIT          bool
	MigrationsDir string
}

// load recupération des variables d'env nécessaire à la connexion à postgresql
func (p *GormConfig) load() error {
	if err := checkEnvVarsExists([]string{"DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_DATABASE"}); err != nil {
		return err
	}

	sslMode := "require"
	if os.Getenv("DB_SSL") != "" {
		sslMode = os.Getenv("DB_SSL")
	}

	p.DSN = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s TimeZone=Europe/Paris", os.Getenv("DB_HOST"), os.Getenv("DB_PORT"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_DATABASE"), sslMode)
	p.INIT = os.Getenv("DB_INIT") == "true"
	p.MigrationsDir = os.Getenv("MIGRATION_DIR")
	return nil
}

// checkEnvVarsExists verifie qu'une list de variables d'env ne sont pas vide
func checkEnvVarsExists(envVars []string) error {
	var err error
	for _, ev := range envVars {
		if len(os.Getenv(ev)) == 0 {
			errV := fmt.Errorf("env var %s is required", ev)
			if err == nil {
				err = errV
				continue
			}
			err = fmt.Errorf("%s, %v", err.Error(), errV)
		}
	}

	return err
}
