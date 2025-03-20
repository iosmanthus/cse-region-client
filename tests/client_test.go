package tests

import (
	"context"
	"encoding/hex"
	"flag"
	"log"
	"strings"
	"testing"

	"github.com/iosmanthus/cse-region-client"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/pkg/caller"
)

var (
	pdAddrs = flag.String("pd-addrs", "127.0.0.1:2379", "pd addrs")
)

// TODO(iosmanthus): refactor this suite as a unit test.
func TestCSE(t *testing.T) {
	suite.Run(t, new(cseSuite))
}

type cseSuite struct {
	suite.Suite
	pdCli pd.Client
}

func (s *cseSuite) SetupTest() {
	pdCli, err := pd.NewClient(caller.TestComponent, strings.Split(*pdAddrs, ","), pd.SecurityOption{})
	s.Nil(err)
	pdCli, err = cse.NewClient(pdCli, nil, nil)
	s.Nil(err)
	s.pdCli = tikv.NewCodecPDClient(tikv.ModeTxn, pdCli)
}

func (s *cseSuite) TearDownTest() {
	s.pdCli.Close()
}

func (s *cseSuite) TestGetRegion() {
	key, err := hex.DecodeString("780000016d44444c5461626c65ff56657273696f6e00fe0000000000000073")
	s.Nil(err)
	currentRegion, err := s.pdCli.GetRegion(context.Background(), key)
	s.Nil(err)
	s.NotNil(currentRegion)
	s.LessOrEqual(currentRegion.Meta.StartKey, key)
	s.Less(key, currentRegion.Meta.EndKey)
	prevRegion, err := s.pdCli.GetPrevRegion(context.Background(), key)
	s.Nil(err)
	s.Equal(prevRegion.Meta.EndKey, currentRegion.Meta.StartKey)
}

func BenchmarkGetRegionByPD(b *testing.B) {
	pdCli, err := pd.NewClient(caller.TestComponent, strings.Split(*pdAddrs, ","), pd.SecurityOption{})
	if err != nil {
		b.Fatal(err)
	}
	pdCli = tikv.NewCodecPDClient(tikv.ModeTxn, pdCli)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		region, err := pdCli.GetRegion(context.Background(), []byte("780000016d44444c5461626c65ff56657273696f6e00fe0000000000000073"))
		if err != nil {
			b.Fatal(err)
		}
		if region == nil {
			log.Fatalln("region is nil")
		}
	}
	b.StopTimer()
	pdCli.Close()
}

func BenchmarkGetRegionByCSE(b *testing.B) {
	pdCli, err := pd.NewClient(caller.TestComponent, strings.Split(*pdAddrs, ","), pd.SecurityOption{})
	if err != nil {
		b.Fatal(err)
	}
	pdCli, err = cse.NewClient(pdCli, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	pdCli = tikv.NewCodecPDClient(tikv.ModeTxn, pdCli)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		region, err := pdCli.GetRegion(context.Background(), []byte("780000016d44444c5461626c65ff56657273696f6e00fe0000000000000073"))
		if err != nil {
			b.Fatal(err)
		}
		if region == nil {
			log.Fatalln("region is nil")
		}
	}
	b.StopTimer()
	pdCli.Close()
}
