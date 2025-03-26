package log

import (
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"mongo-checker/internal/config"
	"mongo-checker/utils/timeutil"
)

var Logger *ZapLogger

func New(c *config.Config) {
	loglevel := zapcore.InfoLevel
	if c.Debug {
		loglevel = zapcore.DebugLevel
	}

	lumberJackLogger := &lumberjack.Logger{
		Filename:   c.LogPath + "/checker.log",
		MaxSize:    20,
		MaxBackups: 999,
		Compress:   true,
	}

	writeSyncer := zapcore.AddSync(lumberJackLogger)
	timeEncoder := zapcore.TimeEncoderOfLayout(timeutil.CSTLayout)
	cfg := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    customLevelEncoder,
		EncodeTime:     timeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   customCallerEncoder,
	}
	encoder := zapcore.NewConsoleEncoder(cfg)
	core := zapcore.NewCore(encoder, writeSyncer, loglevel)
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1)).Sugar()
	Logger = NewZapLogger(logger)
}

func customLevelEncoder(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	levelString := "[" + level.CapitalString() + "]"
	enc.AppendString(levelString)
}

func customCallerEncoder(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	if caller.Defined {
		enc.AppendString("[" + caller.TrimmedPath() + "]")
	} else {
		enc.AppendString("[undefined]")
	}
}

type ZapLogger struct {
	logger *zap.SugaredLogger
}

func NewZapLogger(logger *zap.SugaredLogger) *ZapLogger {
	return &ZapLogger{logger: logger}
}

// Printf formats according to a format specifier and writes to the logger.
func (l *ZapLogger) Printf(format string, v ...interface{}) {
	l.logger.Infof(format, v...)
}

// Print calls Printf with the default message format.
func (l *ZapLogger) Print(v ...interface{}) {
	l.logger.Info(v...)
}

// Println calls Print with a newline.
func (l *ZapLogger) Println(v ...interface{}) {
	l.logger.Info(v...)
}

// Fatal calls Print followed by a call to os.Exit(1).
func (l *ZapLogger) Fatal(v ...interface{}) {
	l.logger.Fatal(v...)
}

// Fatalf is equivalent to Printf followed by a call to os.Exit(1).
func (l *ZapLogger) Fatalf(format string, v ...interface{}) {
	l.logger.Fatalf(format, v...)
}

// Fatalln is equivalent to Fatal.
func (l *ZapLogger) Fatalln(v ...interface{}) {
	l.logger.Fatal(v...)
}

// Panic is equivalent to Print followed by a call to panic().
func (l *ZapLogger) Panic(v ...interface{}) {
	l.logger.Panic(v...)
}

// Panicf is equivalent to Printf followed by a call to panic().
func (l *ZapLogger) Panicf(format string, v ...interface{}) {
	l.logger.Panicf(format, v...)
}

func (l *ZapLogger) Debugf(format string, args ...interface{}) {
	l.logger.Debugf(format, args...)
}

func (l *ZapLogger) Infof(format string, args ...interface{}) {
	l.logger.Infof(format, args...)
}

func (l *ZapLogger) Warnf(format string, args ...interface{}) {
	l.logger.Warnf(format, args...)
}

func (l *ZapLogger) Errorf(format string, args ...interface{}) {
	l.logger.Errorf(format, args...)
}

func (l *ZapLogger) Debug(format string, args ...interface{}) {
	l.logger.Debugf(format, args...)
}

func (l *ZapLogger) Info(format string, args ...interface{}) {
	l.logger.Infof(format, args...)
}

func (l *ZapLogger) Warn(format string, args ...interface{}) {
	l.logger.Warnf(format, args...)
}

func (l *ZapLogger) Error(format string, args ...interface{}) {
	l.logger.Errorf(format, args...)
}

func (l *ZapLogger) Sync() {
	_ = l.logger.Sync()
}
