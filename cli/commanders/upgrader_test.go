package commanders_test

import (
	"errors"
	"io"
	"io/ioutil"
	"os"

	"github.com/greenplum-db/gpupgrade/cli/commanders"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/idl/mock_idl"
	"github.com/greenplum-db/gpupgrade/testutils"
	"github.com/greenplum-db/gpupgrade/utils"

	"github.com/golang/mock/gomock"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("reporter", func() {
	var (
		client *mock_idl.MockCliToHubClient
		ctrl   *gomock.Controller

		hubClient    *testutils.MockHubClient
		clientStream *mock_idl.MockCliToHub_UpgradeConvertMasterClient
		upgrader     *commanders.Upgrader
		testStderr   *gbytes.Buffer
	)

	BeforeEach(func() {
		_, testStderr, _ = testhelper.SetupTestLogger()

		ctrl = gomock.NewController(GinkgoT())
		client = mock_idl.NewMockCliToHubClient(ctrl)
		clientStream = mock_idl.NewMockCliToHub_UpgradeConvertMasterClient(ctrl)
		hubClient = testutils.NewMockHubClient()
		upgrader = commanders.NewUpgrader(hubClient)
	})

	AfterEach(func() {
		utils.System = utils.InitializeSystemFunctions()
		defer ctrl.Finish()
	})

	Describe("ConvertMaster", func() {
		It("writes STDOUT and STDERR chunks in the order they are received", func() {
			client.EXPECT().UpgradeConvertMaster(
				gomock.Any(),
				&idl.UpgradeConvertMasterRequest{},
			).Return(clientStream, nil)
			gomock.InOrder(
				clientStream.EXPECT().Recv().Return(&idl.Chunk{
					Buffer: []byte("my string1"),
					Type:   idl.Chunk_STDOUT,
				}, nil),
				clientStream.EXPECT().Recv().Return(&idl.Chunk{
					Buffer: []byte("my error"),
					Type:   idl.Chunk_STDERR,
				}, nil),
				clientStream.EXPECT().Recv().Return(&idl.Chunk{
					Buffer: []byte("my string2"),
					Type:   idl.Chunk_STDOUT,
				}, nil),
				clientStream.EXPECT().Recv().Return(nil, io.EOF),
			)

			rOut, wOut, _ := os.Pipe()
			rErr, wErr, _ := os.Pipe()
			tmpOut := os.Stdout
			tmpErr := os.Stderr
			defer func() {
				os.Stdout = tmpOut
				os.Stderr = tmpErr
			}()
			os.Stdout = wOut
			os.Stderr = wErr
			go func() {
				defer GinkgoRecover()
				defer wOut.Close()
				defer wErr.Close()
				err := commanders.NewUpgrader(client).ConvertMaster()
				Expect(err).To(BeNil())
			}()
			allOut, _ := ioutil.ReadAll(rOut)
			allErr, _ := ioutil.ReadAll(rErr)

			Expect(string(allOut)).To(Equal("my string1my string2"))
			Expect(string(allErr)).To(Equal("my error"))
		})

		It("returns an error when a non io.EOF error is encountered", func() {
			client.EXPECT().UpgradeConvertMaster(
				gomock.Any(),
				&idl.UpgradeConvertMasterRequest{},
			).Return(clientStream, nil)
			clientStream.EXPECT().Recv().Return(nil, io.ErrUnexpectedEOF)

			err := commanders.NewUpgrader(client).ConvertMaster()
			Expect(err).To(Equal(io.ErrUnexpectedEOF))
		})

		It("reports failure when command fails to connect to the hub", func() {
			client.EXPECT().UpgradeConvertMaster(
				gomock.Any(),
				&idl.UpgradeConvertMasterRequest{},
			).Return(nil, errors.New("something bad happened"))
			err := commanders.NewUpgrader(client).ConvertMaster()
			Expect(err).ToNot(BeNil())
			Eventually(testStderr).Should(gbytes.Say("ERROR - Unable to connect to hub"))
		})
	})

	Describe("ConvertPrimaries", func() {
		It("returns no error when the hub returns no error", func() {
			err := upgrader.ConvertPrimaries()
			Expect(err).ToNot(HaveOccurred())
		})

		It("returns an error when the hub returns an error", func() {
			hubClient.Err = errors.New("hub error")

			err := upgrader.ConvertPrimaries()
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("CopyMasterDataDir", func() {
		It("returns no error when copying master data directory successfully", func() {
			err := upgrader.CopyMasterDataDir()
			Expect(err).ToNot(HaveOccurred())

			Expect(hubClient.UpgradeCopyMasterDataDirRequest).To(Equal(&idl.UpgradeCopyMasterDataDirRequest{}))
		})

		It("returns an error when copying master data directory cannot be shared", func() {
			hubClient.Err = errors.New("test share oids failed")

			err := upgrader.CopyMasterDataDir()
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("ReconfigurePorts", func() {
		It("returns nil error when ports are reconfigured successfully", func() {
			err := upgrader.ReconfigurePorts()
			Expect(err).ToNot(HaveOccurred())

			Expect(hubClient.UpgradeReconfigurePortsRequest).To(Equal(&idl.UpgradeReconfigurePortsRequest{}))
		})

		It("returns error when ports cannot be reconfigured", func() {
			hubClient.Err = errors.New("reconfigure ports failed")

			err := upgrader.ReconfigurePorts()
			Expect(err).To(HaveOccurred())
		})
	})
})
