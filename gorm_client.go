package metaventus_gorm_adapters

import (
	"bufio"
	"github.com/rs/zerolog/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"os"
	"path/filepath"
	"strings"
)

type GormClient struct {
	DB            *gorm.DB
	migrationsDir string
}

func New(cfg *GormConfig, migrationStructs ...any) (*GormClient, error) {
	db, err := gorm.Open(postgres.Open(cfg.DSN), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(0)
	client := &GormClient{DB: db}

	if err = client.reset(cfg.INIT); err != nil {
		return nil, err
	}

	if err = client.migrations(migrationStructs); err != nil {
		return nil, err
	}

	if err = client.scripts(); err != nil {
		return nil, err
	}

	return client, nil
}

// reset efface la base de donnée
func (pgc *GormClient) reset(resetDatabase bool) error {
	if !resetDatabase {
		return nil
	}

	err := pgc.DB.Exec(`
		DO $$ DECLARE
			r RECORD;
		BEGIN
			FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
				EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
			END LOOP;
		END $$;
	`).Error

	return err
}

func (pgc *GormClient) migrations(migrationStructs ...any) error {
	// Effectuez la migration automatique pour créer toutes les tables
	err := pgc.DB.AutoMigrate(migrationStructs...)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to auto migrate database")
	}
	log.Info().Caller().Msg("Successfully created connection to database")

	return nil
}

// rollBack joue les scripts de migrations présent dans le dossier migrations.
// les migrations gorms ne gérant pas les suppressions de tables ou de colonnes, un script manuel est obligatoire.
// peu être utiles dans certaines autres optimisations de la base de données
func (pgc *GormClient) scripts() error {
	if pgc.migrationsDir == "" {
		return nil
	}
	files, err := os.ReadDir(pgc.migrationsDir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".sql") {
			filePath := filepath.Join(pgc.migrationsDir, file.Name())
			log.Printf("Applying migration: %s", filePath)

			if err = pgc.executeSQLFile(filePath); err != nil {
				return err
			}
		}
	}
	return nil
}

// executeSQLFile execute un fichier .sql
func (pgc *GormClient) executeSQLFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var sqlStmt strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		sqlStmt.WriteString(line + "\n")
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	err = pgc.DB.Exec(sqlStmt.String()).Error
	if err != nil {
		return err
	}
	return nil
}

func (pgc *GormClient) Close() error {
	db, err := pgc.DB.DB()
	if err != nil {
		return err
	}

	return db.Close()
}
