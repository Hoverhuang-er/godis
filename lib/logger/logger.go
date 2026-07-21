package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const logFileName = "godis.log"

// Output levels
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARNING
	ERROR
	FATAL
)

const (
	defaultCallerDepth = 2
)

// Settings stores config for Logger
type Settings struct {
	Path       string `yaml:"path"`
	Name       string `yaml:"name"`
	Ext        string `yaml:"ext"`
	TimeFormat string `yaml:"time-format"`
}

// ILogger defines the methods that any logger should implement
type ILogger interface {
	Output(level LogLevel, callerDepth int, msg string)
}

// Logger wraps a zap.Logger for high-performance JSON logging
type Logger struct {
	zap  *zap.Logger
	slog *slog.Logger
}

var DefaultLogger ILogger

func init() {
	l, err := NewLogger()
	if err != nil {
		panic(err)
	}
	DefaultLogger = l
}

// NewLogger creates a JSON logger writing to both stdout and godis.log.
// Uses zap for performance with slog compatibility.
func NewLogger() (*Logger, error) {
	// Open log file
	logDir := getDefaultLogDir()
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}
	logPath := filepath.Join(logDir, logFileName)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	// Multi-writer: stdout + file
	mw := io.MultiWriter(os.Stdout, logFile)

	// Zap JSON encoder config
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderCfg.CallerKey = "caller"
	encoderCfg.EncodeCaller = zapcore.ShortCallerEncoder

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.AddSync(mw),
		zapcore.ErrorLevel, // default: only error level
	)

	zapLogger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))

	return &Logger{
		zap:  zapLogger,
		slog: slog.New(&zapSlogHandler{zap: zapLogger, enc: zapcore.NewJSONEncoder(encoderCfg)}),
	}, nil
}

func getDefaultLogDir() string {
	// Use ./logs/ relative to working directory
	if _, err := os.Stat("logs"); err == nil {
		return "logs"
	}
	tmpDir := os.TempDir()
	return filepath.Join(tmpDir, "godis", "logs")
}

// SetLevel dynamically changes the minimum log level.
// LevelDebug, LevelInfo, LevelWarn, LevelError
func SetLevel(level slog.Level) {
	if l, ok := DefaultLogger.(*Logger); ok {
		// Rebuild core with new level
		var zapLevel zapcore.Level
		switch level {
		case slog.LevelDebug:
			zapLevel = zapcore.DebugLevel
		case slog.LevelInfo:
			zapLevel = zapcore.InfoLevel
		case slog.LevelWarn:
			zapLevel = zapcore.WarnLevel
		default:
			zapLevel = zapcore.ErrorLevel
		}

		encoderCfg := zap.NewProductionEncoderConfig()
		encoderCfg.TimeKey = "timestamp"
		encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		encoderCfg.CallerKey = "caller"
		encoderCfg.EncodeCaller = zapcore.ShortCallerEncoder

		logDir := getDefaultLogDir()
		logPath := filepath.Join(logDir, logFileName)
		logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return
		}
		mw := io.MultiWriter(os.Stdout, logFile)

		core := zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderCfg),
			zapcore.AddSync(mw),
			zapLevel,
		)

		l.zap = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
		l.slog = slog.New(&zapSlogHandler{zap: l.zap, enc: zapcore.NewJSONEncoder(encoderCfg)})
	}
}

// zapSlogHandler bridges slog to zap's JSON encoder
type zapSlogHandler struct {
	zap *zap.Logger
	enc zapcore.Encoder
}

func (h *zapSlogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.zap.Core().Enabled(levelToZap(level))
}

func (h *zapSlogHandler) Handle(ctx context.Context, r slog.Record) error {
	fields := make([]zap.Field, 0, r.NumAttrs())
	r.Attrs(func(attr slog.Attr) bool {
		if attr.Key != "" {
			fields = append(fields, zap.String(attr.Key, attr.Value.String()))
		}
		return true
	})

	ce := h.zap.Core().Check(zapcore.Entry{
		Level:   levelToZap(r.Level),
		Time:    r.Time,
		Message: r.Message,
	}, nil)
	if ce != nil {
		ce.Write(fields...)
	}
	return nil
}

func (h *zapSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *zapSlogHandler) WithGroup(name string) slog.Handler {
	return h
}

func levelToZap(l slog.Level) zapcore.Level {
	switch l {
	case slog.LevelDebug:
		return zapcore.DebugLevel
	case slog.LevelInfo:
		return zapcore.InfoLevel
	case slog.LevelWarn:
		return zapcore.WarnLevel
	case slog.LevelError:
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

// Output implements ILogger
func (l *Logger) Output(level LogLevel, callerDepth int, msg string) {
	switch level {
	case DEBUG:
		l.zap.Debug(msg)
	case INFO:
		l.zap.Info(msg)
	case WARNING:
		l.zap.Warn(msg)
	case ERROR, FATAL:
		l.zap.Error(msg)
	}
}

// Slog returns the slog.Logger for structured logging
func (l *Logger) Slog() *slog.Logger {
	return l.slog
}

// Zap returns the underlying zap.Logger for high-performance use
func (l *Logger) Zap() *zap.Logger {
	return l.zap
}

// Setup creates a new logger with given settings (backward compatible)
func Setup(settings *Settings) {
	l, err := NewLogger()
	if err != nil {
		panic(err)
	}
	DefaultLogger = l
	if settings != nil && settings.Path != "" {
		logFile, err := os.OpenFile(
			filepath.Join(settings.Path, logFileName),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return
		}
		mw := io.MultiWriter(os.Stdout, logFile)
		encoderCfg := zap.NewProductionEncoderConfig()
		encoderCfg.TimeKey = "timestamp"
		encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		encoderCfg.CallerKey = "caller"
		encoderCfg.EncodeCaller = zapcore.ShortCallerEncoder

		core := zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderCfg),
			zapcore.AddSync(mw),
			zapcore.ErrorLevel,
		)
		l.zap = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
		l.slog = slog.New(&zapSlogHandler{zap: l.zap, enc: zapcore.NewJSONEncoder(encoderCfg)})
	}
}

func getCallerInfo(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "???"
	}
	return filepath.Base(file) + ":" + itoa(line)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// Debug logs in format of [DEBUG] + msg
func Debug(v ...any) {
	if l, ok := DefaultLogger.(*Logger); ok {
		l.zap.Sugar().Debug(v...)
	}
}

// Debugf logs formatted debug message
func Debugf(format string, v ...any) {
	if l, ok := DefaultLogger.(*Logger); ok {
		l.zap.Sugar().Debugf(format, v...)
	}
}

// Info logs in format of [INFO] + msg
func Info(v ...any) {
	if l, ok := DefaultLogger.(*Logger); ok {
		l.zap.Sugar().Info(v...)
	}
}

// Infof logs formatted info message
func Infof(format string, v ...any) {
	if l, ok := DefaultLogger.(*Logger); ok {
		l.zap.Sugar().Infof(format, v...)
	}
}

// Warn logs in format of [WARN] + msg
func Warn(v ...any) {
	if l, ok := DefaultLogger.(*Logger); ok {
		l.zap.Sugar().Warn(v...)
	}
}

// Warnf logs formatted warning message
func Warnf(format string, v ...any) {
	if l, ok := DefaultLogger.(*Logger); ok {
		l.zap.Sugar().Warnf(format, v...)
	}
}

// Error logs in format of [ERROR] + msg
func Error(v ...any) {
	if l, ok := DefaultLogger.(*Logger); ok {
		l.zap.Sugar().Error(v...)
	}
}

// Errorf logs formatted error message
func Errorf(format string, v ...any) {
	if l, ok := DefaultLogger.(*Logger); ok {
		l.zap.Sugar().Errorf(format, v...)
	}
}

// Fatal logs and exits
func Fatal(v ...any) {
	if l, ok := DefaultLogger.(*Logger); ok {
		l.zap.Sugar().Fatal(v...)
	}
}
