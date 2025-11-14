// battery
// Copyright (C) 2016-2017,2019,2023 Karol 'Kenji Takahashi' Woźniak
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
// TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
// OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package battery

import (
	"bytes"
	"fmt"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

func sysctl(mib []int32, out unsafe.Pointer, n uintptr) unix.Errno {
	_, _, e := unix.Syscall6(
		unix.SYS___SYSCTL,
		uintptr(unsafe.Pointer(&mib[0])),
		uintptr(len(mib)),
		uintptr(out),
		uintptr(unsafe.Pointer(&n)),
		uintptr(unsafe.Pointer(nil)),
		0,
	)
	return e
}

var errValueNotFound = fmt.Errorf("Value not found")

func setErr(err *error, newErr error) {
	if *err == errValueNotFound {
		*err = newErr
	}
}

var sensorW = [4]int32{
	2,  // SENSOR_VOLTS_DC (uV)
	5,  // SENSOR_WATTS (uW)
	7,  // SENSOR_WATTHOUR (uWh)
	10, // SENSOR_INTEGER
}

const (
	sensorA  = 6 // SENSOR_AMPS (uA)
	sensorAH = 8 // SENSOR_AMPHOUR (uAh)
)

type sensorStatus int32

const (
	unspecified sensorStatus = iota
	ok
	warning
	critical
	unknown
)

type sensor struct {
	desc   [32]byte
	tv     [16]byte // struct timeval
	value  int64
	typ    [4]byte // enum sensor_type
	status sensorStatus
	numt   int32
	flags  int32
}

func (s *sensor) readValue(div float64) (float64, error) {
	if s.status == unknown {
		return 0, fmt.Errorf("Unknown value received")
	}

	return float64(s.value) / div, nil
}

func (s *sensor) handleA(factor float64, field *float64, fieldErr *error) {
	*field, *fieldErr = s.readValue(1000)
	if *fieldErr == nil {
		*field *= factor
	}
}

func (s *sensor) handleAH(factor float64, factorErr error, field *float64, fieldErr *error) {
	if factorErr != nil {
		*fieldErr = factorErr
		return
	}
	s.handleA(factor, field, fieldErr)
}

type sensordev struct {
	num           int32
	xname         [16]byte
	maxnumt       [23]int32
	sensors_count int32
}

func (sd *sensordev) get() (*Battery, error) {
	var battery Battery
	err := ErrPartial{
		Design:        errValueNotFound,
		Full:          errValueNotFound,
		Current:       errValueNotFound,
		ChargeRate:    errValueNotFound,
		State:         errValueNotFound,
		Voltage:       errValueNotFound,
		DesignVoltage: errValueNotFound,
	}

	mib := []int32{unix.CTL_HW, 11, sd.num, 0, 0}
	var s sensor

	iter := func(maxnumtIdx int32, cb func(string)) {
		mib[3] = maxnumtIdx

		for i := int32(0); i < sd.maxnumt[maxnumtIdx]; i++ {
			mib[4] = i

			if errno := sysctl(mib, unsafe.Pointer(&s), unsafe.Sizeof(s)); errno != 0 {
				setErr(&err.Design, errno)
				setErr(&err.Full, errno)
				setErr(&err.Current, errno)
				setErr(&err.ChargeRate, errno)
				setErr(&err.Voltage, errno)
				setErr(&err.DesignVoltage, errno)
				setErr(&err.State, errno)
			}

			// Convert 0-terminated C-string to a Go string
			cb(string(s.desc[:bytes.IndexByte(s.desc[:], 0)]))
		}
	}

	for _, w := range sensorW {
		mib[3] = w

		iter(w, func(desc string) {
			if strings.HasPrefix(desc, "battery ") {
				battery.State.specific, err.State = desc, nil

				switch desc[8:] {
				case "unknown":
					battery.State.Raw = Unknown
				case "full":
					battery.State.Raw = Full
				case "charging":
					battery.State.Raw = Charging
				case "discharging":
					battery.State.Raw = Discharging
				case "idle":
					battery.State.Raw = Idle
				case "critical":
					battery.State.Raw = Empty
				default:
					battery.State.Raw = Undefined
				}
				return
			}
			switch desc {
			case "rate":
				battery.ChargeRate, err.ChargeRate = s.readValue(1000)
			case "design capacity":
				battery.Design, err.Design = s.readValue(1000)
			case "last full capacity":
				battery.Full, err.Full = s.readValue(1000)
			case "remaining capacity":
				battery.Current, err.Current = s.readValue(1000)
			case "current voltage":
				battery.Voltage, err.Voltage = s.readValue(1000_000)
			case "voltage":
				battery.DesignVoltage, err.DesignVoltage = s.readValue(1000_000)
			}
		})
	}

	if err.DesignVoltage != nil && err.Voltage == nil {
		battery.DesignVoltage, err.DesignVoltage = battery.Voltage, nil
	}
	if err.ChargeRate == errValueNotFound {
		if err.Voltage == nil {
			iter(sensorA, func(desc string) {
				if desc == "rate" {
					s.handleA(battery.Voltage, &battery.ChargeRate, &err.ChargeRate)
				}
			})
		} else {
			err.ChargeRate = err.Voltage
		}
	}
	if err.Design == errValueNotFound || err.Full == errValueNotFound || err.Current == errValueNotFound {
		iter(sensorAH, func(desc string) {
			switch desc {
			case "design capacity":
				s.handleAH(battery.DesignVoltage, err.DesignVoltage, &battery.Design, &err.Design)
			case "last full capacity":
				s.handleAH(battery.Voltage, err.Voltage, &battery.Full, &err.Full)
			case "remaining capacity":
				s.handleAH(battery.Voltage, err.Voltage, &battery.Current, &err.Current)
			}
		})
	}

	return &battery, err
}

func getBatteryAtMIBIndex(i int32) (*Battery, int32, error) {
	mib := []int32{
		unix.CTL_HW,
		11, // HW_SENSORS
		0,
	}
	var sd sensordev
	for ; ; i++ {
		mib[2] = i

		errno := sysctl(mib, unsafe.Pointer(&sd), unsafe.Sizeof(sd))
		if errno == unix.ENOENT {
			break
		}
		if errno != 0 {
			continue
		}
		if bytes.HasPrefix(sd.xname[:], []byte("acpibat")) {
			battery, err := sd.get()
			return battery, i + 1, err
		}
	}
	return nil, i, nil
}

func systemGet(idx int) (*Battery, error) {
	var i int32
	for idxCurr := 0; ; idxCurr++ {
		battery, iNext, err := getBatteryAtMIBIndex(i)
		// If this is the index we look for, grab it.
		// Otherwise just move on, regardless of errors etc.
		if idxCurr == idx {
			return battery, err
		}

		i = iNext
	}

	return nil, ErrNotFound
}

func systemGetAll() ([]*Battery, error) {
	var batteries []*Battery
	var errors Errors

	var i int32
	for {
		battery, iNext, err := getBatteryAtMIBIndex(i)
		if battery == nil && err == nil { // no more batteries
			break
		}
		batteries = append(batteries, battery)
		errors = append(errors, err)

		i = iNext
	}

	return batteries, errors
}
