package logger

import (
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"goimpl/config"
	"os"
	"path"
	"time"
)
var Logger *zap.Logger

func InitGoroutingLogger(cfg *config.LogConfig,logType string,env string) (loggerObj *zap.Logger, err error) {
	ws := getLogWriter(cfg.LogPath, cfg.MaxSize, cfg.Level,logType,env) // 做日志切割第三方包
	encoder := getEncoder() // 日志输出的格式
	var level = new(zapcore.Level)
	err = level.UnmarshalText([]byte(cfg.Level))
	if err != nil {
		return nil,err
	}
	var writeSyncer zapcore.WriteSyncer
	if *level ==zapcore.DebugLevel || *level == zapcore.InfoLevel {
		zapcore.AddSync(os.Stdout)
		writeSyncer = zapcore.NewMultiWriteSyncer(ws,os.Stdout)
	}else {
		writeSyncer = ws
	}
	//core := zapcore.NewCore(encoder, ws, level)
	core := zapcore.NewCore(encoder, writeSyncer, level)

	return zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1)),nil
}



func InitLogger(cfg *config.LogConfig,logType string,env string) (err error) {
	ws := getLogWriter(cfg.LogPath, cfg.MaxSize, cfg.Level,logType,env) // 做日志切割第三方包
	encoder := getEncoder() // 日志输出的格式
	var level = new(zapcore.Level)
	err = level.UnmarshalText([]byte(cfg.Level))
	if err != nil {
		return
	}
	//console print
	var writeSyncer zapcore.WriteSyncer
	if * level == zapcore.DebugLevel || *level == zapcore.InfoLevel{
		zapcore.AddSync(os.Stdout)
		writeSyncer = zapcore.NewMultiWriteSyncer(ws,os.Stdout)
	}else {
		writeSyncer = ws
	}
	core := zapcore.NewCore(encoder, writeSyncer, level)

	Logger = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	return
}


func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder  // 时间字符串
	encoderConfig.TimeKey = "time"
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderConfig.EncodeDuration = zapcore.SecondsDurationEncoder
	encoderConfig.EncodeCaller = zapcore.ShortCallerEncoder  // 函数调用
	return zapcore.NewJSONEncoder(encoderConfig)  // JSON格式
}

func getLogWriter(logpath string, maxSize  uint,level,logType,env string) zapcore.WriteSyncer {
	//lumberJackLogger := &lumberjack.Logger{
	//	Filename:   filename,
	//	MaxSize:    maxSize,
	//	MaxBackups: maxBackup,
	//	MaxAge:     maxAge,
	//}
	logFullPath := path.Join(logpath, env,logType,level)
	hook, err := rotatelogs.New(
		logFullPath+".%Y%m%d%H%M",                   // 没有使用go风格反人类的format格式
		rotatelogs.WithLinkName(logFullPath),      // 生成软链，指向最新日志文件
		rotatelogs.WithRotationCount(maxSize),        // 文件最大保存份数
		rotatelogs.WithRotationTime(1*time.Hour), // 日志切割时间间隔
	)
	if err != nil {
		panic(err)
	}
	return zapcore.AddSync(hook)
}

func Debug(msg string, fields ...zap.Field) {
	Logger.Debug(msg, fields...)  // logger.go
}

func Info(msg string, fields ...zap.Field) {
	Logger.Info(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	Logger.Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	Logger.Error(msg, fields...)
}

func With(fields ...zap.Field) *zap.Logger {
	return Logger.With(fields...)
}

func Sync()  {
	Logger.Sync()
}