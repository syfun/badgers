package badgers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Server represents a inner http server of DB.
type Server struct {
	db  *DB
	r   *gin.Engine
	srv *http.Server
}

// LoadRouter load default routers.
func (db *DB) LoadRouter(r *gin.Engine) {
	r.GET("/items/:key/", getKey(db))
	r.POST("/items/", setKey(db))
	r.DELETE("/items/:key/", deleteKey(db))
	r.GET("/keys/", listKeys(db))
}

// Server get Server from db.
func (db *DB) Server() *Server {
	r := gin.Default()
	db.LoadRouter(r)

	return &Server{db: db, r: r}
}

// Run server.
func (s *Server) Run(addr string) {
	srv := &http.Server{
		Addr:    ":8080",
		Handler: s.r,
	}
	s.srv = srv
	go func() {
		// service connections
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()
}

// Close server.
func (s *Server) Close(ctx context.Context) {
	if err := s.srv.Shutdown(ctx); err != nil {
		log.Fatal("Server Shutdown:", err)
	}
	if err := s.db.Close(); err != nil {
		log.Fatal("Server Shutdown: ", err)
	}
}

func getKey(db *DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.Param("key")
		if key == "" {
			c.JSON(400, gin.H{"error": "need not empty key"})
			return
		}
		val, err := db.Get(key)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				c.JSON(404, gin.H{"error": fmt.Sprintf("key '%v' not found", key)})
				return
			}
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		var v interface{}
		if err := json.Unmarshal(val, &v); err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"value": v})
	}
}

type setReq struct {
	Key   string      `json:"key" bind:"required"`
	Value interface{} `json:"value" bind:"required"`
}

func setKey(db *DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req setReq
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		val, err := json.Marshal(&req.Value)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		if err := db.Set(req.Key, val); err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.String(201, "")
	}
}

func deleteKey(db *DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.Param("key")
		if key == "" {
			c.JSON(400, gin.H{"error": "need not empty key"})
			return
		}
		if err := db.Delete(key); err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				c.JSON(404, gin.H{"error": fmt.Sprintf("key '%v' not found", key)})
				return
			}
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.String(204, "")
	}
}

func listKeys(db *DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		prefix := c.Query("prefix")
		keys, err := db.ListKeys(prefix)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		if keys == nil {
			keys = []string{}
		}
		c.JSON(200, gin.H{"keys": keys})
	}
}
