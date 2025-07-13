package loader

import (
	"reflect"
	"testing"
	"time"
)

func TestGetDateInRange(t *testing.T) {
	type args struct {
		beginningDate time.Time
		endDate       time.Time
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "test",
			args: args{
				beginningDate: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				endDate:       time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
			},
			want: []string{"01012021", "02012021"},
		},
		{
			name: "test",
			args: args{
				beginningDate: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				endDate:       time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC),
			},
			want: []string{"01012021", "02012021", "03012021", "04012021", "05012021"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getDateInRange(tt.args.beginningDate, tt.args.endDate); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetDateInRange() = %v, want %v", got, tt.want)
			}
		})
	}
}
