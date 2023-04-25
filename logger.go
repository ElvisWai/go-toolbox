package go_toolbox

import (
	"encoding/json"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"os"
	"time"
)

type ZapLogger struct {
	Filename   string
	MaxSize    int
	MaxBackups int
	MaxAge     int
	Compress   bool
}

// LogConsoleSeparator 分隔符
const LogConsoleSeparator string = "|-|"

var Logger *zap.Logger

func GetLogPrefix(traceId string, msgList ...string) string {
	prefix := traceId
	for _, msg := range msgList {
		prefix = msg
	}

	return prefix
}

// NewZapLogger 初始化 Zap Logger
func NewZapLogger(filename string, maxSize int, maxBackups int, maxAge int, compress bool) {
	// 日志滚动包
	hook := lumberjack.Logger{
		Filename:   filename,
		MaxSize:    maxSize,
		MaxBackups: maxBackups,
		MaxAge:     maxAge,
		Compress:   compress,
	}
	// 编码配置
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:       "time",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "linenum",
		FunctionKey:   "function",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.CapitalLevelEncoder,
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
		},
		EncodeDuration: func(d time.Duration, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendInt64(int64(d) / 1000000)
		},
		EncodeCaller: func(caller zapcore.EntryCaller, encoder zapcore.PrimitiveArrayEncoder) {
			encoder.AppendString("\"" + caller.String() + "\"")
		},
		EncodeName: zapcore.FullNameEncoder,
		NewReflectedEncoder: func(writer io.Writer) zapcore.ReflectedEncoder {
			// 使用 github.com/goccy/go-json 代替 encoding/json
			// 提升 json 解析性能
			enc := json.NewEncoder(writer)
			enc.SetEscapeHTML(false)
			return enc
		},
		ConsoleSeparator: LogConsoleSeparator,
	}
	// 设置日志级别
	atomicLevel := zap.NewAtomicLevel()
	atomicLevel.SetLevel(zap.InfoLevel)
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),                                        // 编码器配置
		zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), zapcore.AddSync(&hook)), // 打印到控制台和文件
		atomicLevel, // 日志级别
	)
	// 开启开发模式，堆栈跟踪
	caller := zap.AddCaller()
	// 开启文件及行号
	development := zap.Development()
	// 设置初始化字段
	//field := zap.Fields(zap.String("serviceName", ServiceName))
	// 构造日志
	Logger = zap.New(core, caller, development)
}
