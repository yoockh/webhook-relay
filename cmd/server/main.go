package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"webhook-relay/internal/config"
	"webhook-relay/internal/handler"
	"webhook-relay/internal/mock"
	"webhook-relay/internal/model"
	"webhook-relay/internal/repository"
	"webhook-relay/internal/service"
)

func main() {
	cfg := config.Load()

	db, err := cfg.OpenDB()
	if err != nil {
		panic("failed to connect database: " + err.Error())
	}
	if err := db.AutoMigrate(&model.Customer{}, &model.Event{}, &model.DeliveryAttempt{}); err != nil {
		panic("failed to migrate database: " + err.Error())
	}

	repo := repository.New(db)
	dispatcher := service.NewDispatcher(repo)
	events := handler.New(repo, dispatcher)

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})
	events.Register(e)

	// Optional in-process mock customer endpoint for manual testing.
	if os.Getenv("ENABLE_MOCK") != "" {
		mock.New().Register(e)
	}

	// Start the server, then block until a termination signal arrives.
	go func() {
		if err := e.Start(":" + cfg.Port); err != nil && err != http.ErrServerClosed {
			e.Logger.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	// Graceful shutdown: stop accepting requests, then stop the workers.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		e.Logger.Error(err)
	}
	dispatcher.Shutdown()
}
