package cmd

import (
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/rueian/pgcapture/pkg/sink"
	"github.com/rueian/pgcapture/pkg/source"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var ProfilerListenAddr string

func init() {
	rootCmd.Flags().StringVarP(&ProfilerListenAddr, "ProfilerListenAddr", "", "localhost:6060", "golang profiler http endpoint")
}

var rootCmd = &cobra.Command{
	Use: "pgcapture",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		go func() {
			if ProfilerListenAddr != "" {
				log.Println(http.ListenAndServe(ProfilerListenAddr, nil))
			}
		}()
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func sourceToSink(src source.Source, sk sink.Sink) (err error) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// 讓 restart 的時候可以從上次斷掉的 change 來繼續做
	lastCheckPoint, err := sk.Setup()
	if err != nil {
		return err
	}

	// 這邊會從 sink 拿到的最新的紀錄，讓 source 開始拉資料
	changes, err := src.Capture(lastCheckPoint)
	if err != nil {
		return err
	}

	go func() {
		// 將 source 拉到的資料送到 sink
		checkpoints := sk.Apply(changes)
		for cp := range checkpoints {
			// source 會紀錄 sink 讀取到哪裡了
			src.Commit(cp)
		}
	}()

	<-signals
	logrus.Info("receive signal, stopping...")
	sk.Stop()
	src.Stop()
	logrus.Info("receive signal, stopped")
	if err := sk.Error(); err != nil {
		return err
	}
	if err := src.Error(); err != nil {
		return err
	}
	return nil
}

func serveGRPC(desc *grpc.ServiceDesc, addr string, impl interface{}, clean func()) (err error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	server.RegisterService(desc, impl)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		<-signals
		logrus.Info("receive signal, stopping grpc server...")
		server.GracefulStop()
		logrus.Info("receive signal, cleaning up...")
		clean()
		logrus.Info("receive signal, stopped")
	}()

	return server.Serve(lis)
}

func trimSlot(topic string) string {
	topic = strings.TrimPrefix(topic, "persistent://public/")
	topic = strings.ReplaceAll(topic, "/", "_")
	topic = strings.ReplaceAll(topic, "-", "_")
	return topic
}
