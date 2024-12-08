package testx

import (
	"fmt"
	"testing"
)

func Name(i int) string {
	return fmt.Sprintf("test_#%03d", i)
}

func RunIth(i int, t *testing.T, f func(t *testing.T)) {
	t.Run(Name(i), f)
}

func RunTestsParallel[TEST any](tx *Tx, tests []TEST, runFnc func(tx *Tx, test TEST)) {
	for i, test := range tests {
		tx.T().Run(fmt.Sprintf("test_%03d", i), func(t *testing.T) {
			runFnc(NewTx(t), test)
		})
	}
}
