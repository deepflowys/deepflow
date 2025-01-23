/*
 * Copyright (c) 2024 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package utils

import (
	. "encoding/binary"
	"fmt"
	"math"
	"net"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
)

func UintMin(x, y uint) uint {
	if x < y {
		return x
	}
	return y
}

func Mac2Uint64(mac net.HardwareAddr) uint64 {
	return uint64(BigEndian.Uint16(mac[:]))<<32 | uint64(BigEndian.Uint32(mac[2:]))
}

func Uint64ToMac(v uint64) net.HardwareAddr {
	bytes := [8]byte{}
	BigEndian.PutUint64(bytes[:], v)
	return net.HardwareAddr(bytes[2:])
}

func FindInterfaceByIp(ip string) *net.Interface {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	for _, iface := range interfaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			if strings.Contains(addr.String(), ip) {
				return &iface
			}
		}
	}
	return nil
}

func IsMulticast(mac []byte) bool {
	return mac[0]&0x1 == 1
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func Abs(n time.Duration) time.Duration {
	m := n >> 63
	return (n ^ m) - m
}

func IpToUint32(ip net.IP) uint32 {
	if len(ip) != net.IPv4len {
		return 0
	}
	return BigEndian.Uint32(ip)
}

func IpFromUint32(ipInt uint32) net.IP {
	ip := make([]byte, net.IPv4len)
	BigEndian.PutUint32(ip, ipInt)
	return ip
}

func GetIpHash(ip net.IP) uint32 {
	ipHash := uint32(0)
	for i := 0; i < len(ip); i += 4 {
		ipHash ^= *(*uint32)(unsafe.Pointer(&ip[i]))
	}
	return ipHash
}

func Bool2Int(b bool) int {
	if b {
		return 1
	}
	return 0
}

func Bool2UInt32(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}

func Bool2UInt8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func MaskLenToNetmask(mask uint32) uint32 {
	return (math.MaxUint32) << (32 - mask)
}

func ParserStringIp(ipStr string) net.IP {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return nil
	}
	if ipv4 := ip.To4(); ipv4 != nil {
		return ipv4
	}
	return ip
}

func ParserStringIpV4(ipStr string) net.IP {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return nil
	}
	if ip = ip.To4(); ip == nil {
		return nil
	}
	return ip
}

func IpNetmaskFromStringCIDR(ipStr string) (net.IP, uint32, error) {
	_, r, err := net.ParseCIDR(ipStr)
	if err != nil {
		return nil, 0, err
	}
	maskInt, _ := r.Mask.Size()
	return r.IP, uint32(maskInt), nil
}

func IPv4ToBinary(ip uint32) string {
	var buf [32]byte
	for i := uint32(0); i < 32; i++ {
		if ip&(1<<i) != 0 {
			buf[31-i] = '1'
		} else {
			buf[31-i] = '0'
		}
	}
	return string(buf[:])
}

func IPv6ToBinary(ip net.IP) string {
	if len(ip) != 16 {
		panic(fmt.Sprintf("Invalid IPv6 address %v", ip))
	}
	var buf [128]byte
	for i := uint8(0); i < 128; i++ {
		if ip[15-i/8]&(1<<(i%8)) != 0 {
			buf[127-i] = '1'
		} else {
			buf[127-i] = '0'
		}
	}
	return string(buf[:])
}

// 运行环境信息
type RuntimeEnv struct {
	CpuNum     uint32
	MemorySize uint64

	// linux下为uname中Machine字段, 参照 https://en.wikipedia.org/wiki/Uname 和 https://stackoverflow.com/questions/45125516/possible-values-for-uname-m
	Arch string // 如 x86_64, aarch64, ppc64, armv7l, amd64, i686, i386, mips, sun4u

	// OS名称+版本号, 参照 github.com/shirou/gopsutil/v3/host/host_linux.go:PlatformInformationWithContext
	OS string // 如 ubuntu 11.04, centos 7.5.1804, fedora 19, redhat xx, linuxmint xx等

	// linux下为uname的Release字段
	KernelVersion string // 如 4.19.17
}

func GetRuntimeEnv() RuntimeEnv {
	cpuNum, err := cpu.Counts(true)
	if err != nil {
		cpuNum = runtime.NumCPU()
	}
	env := RuntimeEnv{
		CpuNum: uint32(cpuNum),
	}
	if v, err := mem.VirtualMemory(); err == nil {
		env.MemorySize = v.Total
	}
	if info, err := host.Info(); err == nil {
		env.Arch = info.KernelArch
		env.OS = info.Platform + " " + info.PlatformVersion
		env.KernelVersion = strings.Split(info.KernelVersion, "-")[0]
	}
	return env
}

func String(b []byte) (s string) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

func Slice(s string) (b []byte) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pbytes.Data = pstring.Data
	pbytes.Len = pstring.Len
	pbytes.Cap = pstring.Len
	return
}

func GetTraceIdIndex(traceId string, indexTypeIsIncremetalId, formatIsHex bool, incremetalIdStart, incremetalIdLength int) (uint64, error) {
	if !indexTypeIsIncremetalId {
		return DJBHash(17, traceId), nil
	}

	if len(traceId) < incremetalIdLength || incremetalIdLength == 0 {
		return 0, fmt.Errorf("traceId(%s) or incremetalIdLength(%d) is invalid", traceId, incremetalIdLength)
	}

	traceIdLen := len(traceId)
	start := incremetalIdStart
	if incremetalIdStart < 0 {
		start = traceIdLen + incremetalIdStart
		if start < 0 {
			return 0, fmt.Errorf("traceId(%s) or incremetalIdStart(%d) is invalid", traceId, incremetalIdStart)
		}
	}
	end := start + incremetalIdLength
	if end > traceIdLen {
		return 0, fmt.Errorf("traceId(%s) incremetalIdLocation(%d-%d) is beyond traceId length", traceId, start, end)
	}

	traceIdIndex := traceId[start:end]
	var num uint64
	var err error
	if formatIsHex {
		num, err = strconv.ParseUint(traceIdIndex, 16, 64)
	} else {
		num, err = strconv.ParseUint(traceIdIndex, 10, 64)
	}
	if err != nil {
		return 0, fmt.Errorf("parse(%s) to uint64 failed err %s", traceIdIndex, err)
	}
	hash := DJBHash(17, traceId)
	// the lowest 16 bits are set as the hash value of traceId to reduce duplication when filtering data
	return num<<16 | (hash & 0xffff), nil
}

type DataType uint8

// DataType is extensions to reflect.Kind
const (
	DATATYPE_INVALID DataType = iota
	DATATYPE_IntPtr
	DATATYPE_Int8Ptr
	DATATYPE_Int16Ptr
	DATATYPE_Int32Ptr
	DATATYPE_Int64Ptr
	DATATYPE_UintPtr
	DATATYPE_Uint8Ptr
	DATATYPE_Uint16Ptr
	DATATYPE_Uint32Ptr
	DATATYPE_Uint64Ptr
	DATATYPE_StringSlice
	DATATYPE_Float64Slice
	DATATYPE_IP
)

func ToDataType(str string) DataType {
	switch str {
	case "*uint":
		return DATATYPE_UintPtr
	case "*uint8":
		return DATATYPE_Uint8Ptr
	case "*uint16":
		return DATATYPE_Uint16Ptr
	case "*uint32":
		return DATATYPE_Uint32Ptr
	case "*uint64":
		return DATATYPE_Uint64Ptr
	case "*int":
		return DATATYPE_IntPtr
	case "*int8":
		return DATATYPE_Int8Ptr
	case "*int16":
		return DATATYPE_Int16Ptr
	case "*int32":
		return DATATYPE_Int32Ptr
	case "*int64":
		return DATATYPE_Int32Ptr
	case "[]string":
		return DATATYPE_StringSlice
	case "[]float64":
		return DATATYPE_Float64Slice
	case "net.IP":
		return DATATYPE_IP
	default:
		return DATATYPE_INVALID
	}
}

func GetValueByOffsetAndKind(ptr, offset uintptr, kind reflect.Kind, dataType DataType) interface{} {
	fieldAddr := unsafe.Pointer(ptr + offset)

	switch kind {
	case reflect.String:
		return *(*string)(fieldAddr)
	case reflect.Bool:
		return *(*bool)(fieldAddr)
	case reflect.Int:
		return *(*int)(fieldAddr)
	case reflect.Int8:
		return *(*int8)(fieldAddr)
	case reflect.Int16:
		return *(*int16)(fieldAddr)
	case reflect.Int32:
		return *(*int32)(fieldAddr)
	case reflect.Int64:
		return *(*int64)(fieldAddr)
	case reflect.Uint:
		return *(*uint)(fieldAddr)
	case reflect.Uint8:
		return *(*uint8)(fieldAddr)
	case reflect.Uint16:
		return *(*uint16)(fieldAddr)
	case reflect.Uint32:
		return *(*uint32)(fieldAddr)
	case reflect.Uint64:
		return *(*uint64)(fieldAddr)
	case reflect.Float32:
		return *(*float32)(fieldAddr)
	case reflect.Float64:
		return *(*float64)(fieldAddr)
	case reflect.Pointer:
		switch dataType {
		case DATATYPE_Int8Ptr:
			return *(**int8)(fieldAddr)
		case DATATYPE_Int16Ptr:
			return *(**int16)(fieldAddr)
		case DATATYPE_Int32Ptr:
			return *(**int32)(fieldAddr)
		case DATATYPE_Int64Ptr:
			return *(**int64)(fieldAddr)
		case DATATYPE_Uint8Ptr:
			return *(**uint8)(fieldAddr)
		case DATATYPE_Uint16Ptr:
			return *(**uint16)(fieldAddr)
		case DATATYPE_Uint32Ptr:
			return *(**uint32)(fieldAddr)
		case DATATYPE_Uint64Ptr:
			return *(**uint64)(fieldAddr)
		default:
			return nil

		}
	case reflect.Slice:
		switch dataType {
		case DATATYPE_IP:
			return *(*net.IP)(fieldAddr)
		case DATATYPE_StringSlice:
			return *(*[]string)(fieldAddr)
		case DATATYPE_Float64Slice:
			return *(*[]float64)(fieldAddr)
		default:
			return nil
		}
	default:
		return nil
	}
}

// converting uint64, int64 to float64 may cause loss of precision, so the original string needs to be returned.
func ConvertToFloat64(data interface{}) (float64, string, bool) {
	switch v := data.(type) {
	case uint:
		return float64(v), strconv.FormatUint(uint64(v), 10), true
	case uint8:
		return float64(v), strconv.FormatUint(uint64(v), 10), true
	case uint16:
		return float64(v), strconv.FormatUint(uint64(v), 10), true
	case uint32:
		return float64(v), strconv.FormatUint(uint64(v), 10), true
	case uint64:
		return float64(v), strconv.FormatUint(v, 10), true
	case uintptr:
		return float64(v), strconv.FormatUint(uint64(v), 10), true
	case int:
		return float64(v), strconv.FormatInt(int64(v), 10), true
	case int8:
		return float64(v), strconv.FormatInt(int64(v), 10), true
	case int16:
		return float64(v), strconv.FormatInt(int64(v), 10), true
	case int32:
		return float64(v), strconv.FormatInt(int64(v), 10), true
	case int64:
		return float64(v), strconv.FormatInt(v, 10), true
	case float64:
		return v, strconv.FormatFloat(v, 'f', -1, 64), true
	case bool:
		if v {
			return 1, "1", true
		} else {
			return 0, "0", true
		}
	case *uint:
		return float64(*v), strconv.FormatUint(uint64(*v), 10), true
	case *uint8:
		return float64(*v), strconv.FormatUint(uint64(*v), 10), true
	case *uint16:
		return float64(*v), strconv.FormatUint(uint64(*v), 10), true
	case *uint32:
		return float64(*v), strconv.FormatUint(uint64(*v), 10), true
	case *uint64:
		return float64(*v), strconv.FormatUint(*v, 10), true
	case *int:
		return float64(*v), strconv.FormatInt(int64(*v), 10), true
	case *int8:
		return float64(*v), strconv.FormatInt(int64(*v), 10), true
	case *int16:
		return float64(*v), strconv.FormatInt(int64(*v), 10), true
	case *int32:
		return float64(*v), strconv.FormatInt(int64(*v), 10), true
	case *int64:
		return float64(*v), strconv.FormatInt(*v, 10), true
	default:
		return 0, "", false
	}
}

func IsNil(i interface{}) bool {
	if i == nil {
		return true
	}
	vi := reflect.ValueOf(i)
	if vi.Kind() == reflect.Ptr {
		return vi.IsNil()
	}
	return false
}

func EscapeJsonStringToStringBuilder(sb *strings.Builder, str string) {
	for i := 0; i < len(str); i++ {
		c := str[i]
		if c == 0x08 {
			sb.WriteString("\\b")
		} else if c == 0x09 {
			sb.WriteString("\\t")
		} else if c == 0x0A {
			sb.WriteString("\\n")
		} else if c == 0x0C {
			sb.WriteString("\\f")
		} else if c == 0x0D {
			sb.WriteString("\\r")
		} else if c == 0x22 {
			sb.WriteString("\\\"")
		} else if c == 0x5C {
			sb.WriteString("\\\\")
		} else if c >= 0x00 && c <= 0x1F {
			sb.WriteString(fmt.Sprintf("\\u%04x", int(c)))
		} else {
			sb.WriteByte(c)
		}
	}
}

func IsEscapeJsonNeeded(str string) bool {
	for i := 0; i < len(str); i++ {
		c := str[i]
		if (c >= 0x00 && c <= 0x1F) || c == 0x22 || c == 0x5C {
			return true
		}
	}
	return false
}

func EscapeJsonString(str string) string {
	if !IsEscapeJsonNeeded(str) {
		return str
	}
	var result strings.Builder
	EscapeJsonStringToStringBuilder(&result, str)
	return result.String()
}

func CloneStringSlice(strs []string) []string {
	return append([]string{}, strs...)
}

func IndexOf(strs []string, str string) int {
	for i, v := range strs {
		if v == str {
			return i
		}
	}
	return -1
}
