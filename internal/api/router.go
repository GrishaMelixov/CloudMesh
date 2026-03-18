package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

func NewRouter(db *DB) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	r.GET("/health/live", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	r.GET("/health/ready", func(c *gin.Context) {
		if err := db.Ping(c.Request.Context()); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready", "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	analytics := r.Group("/analytics")
	{
		analytics.GET("/stats", func(c *gin.Context) {
			stats, err := db.Stats(c.Request.Context())
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, stats)
		})

		analytics.GET("/services", func(c *gin.Context) {
			rows, err := db.Services(c.Request.Context())
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, rows)
		})

		analytics.GET("/error-rate", func(c *gin.Context) {
			hours := queryInt(c, "hours", 1)
			rows, err := db.ErrorRate(c.Request.Context(), hours)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, rows)
		})

		analytics.GET("/volume", func(c *gin.Context) {
			days := queryInt(c, "days", 1)
			rows, err := db.Volume(c.Request.Context(), days)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, rows)
		})

		analytics.GET("/logs", func(c *gin.Context) {
			service := c.Query("service")
			level := c.Query("level")
			limit := queryInt(c, "limit", 100)
			rows, err := db.RecentLogs(c.Request.Context(), service, level, limit)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, rows)
		})
	}

	return r
}

func queryInt(c *gin.Context, key string, def int) int {
	v := c.Query(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
