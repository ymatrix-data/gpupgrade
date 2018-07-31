package log_test

import (
	"github.com/greenplum-db/gpupgrade/utils/log"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

var _ = Describe("WritePanics", func() {
	It("does not swallow panics", func() {
		panicFunc := func() {
			defer log.WritePanics()
			panic("ahhh")
		}
		Expect(panicFunc).To(Panic())
	})

	It("writes panic information to the gplog file only", func() {
		oldLogger := gplog.GetLogger()
		defer func() { gplog.SetLogger(oldLogger) }()

		testout, testerr, testlog := testhelper.SetupTestLogger()

		panicMsg := "aaahhhhh"
		Expect(func() {
			defer log.WritePanics()
			panic(panicMsg)
		}).To(Panic())

		Expect(testout.Contents()).To(BeEmpty())
		Expect(testerr.Contents()).To(BeEmpty())
		Expect(testlog).To(Say(`encountered panic \("%s"\); stack trace follows`, panicMsg))
	})
})
