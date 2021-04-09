package redis

import (
	"errors"
	"fmt"
	"strconv"
)

var ErrNil =errors.New("redisgo: nil returned")

// Int is a helper that converts a command reply to an integer. If err is not
// equal to nil, then Int returns 0
func Int(reply interface{}, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	switch reply := reply.(type) {
	case int64:
		x := int(reply)
		if int64(x) !=reply {
			return 0, strconv.ErrRange
		}
		return x, nil
	case []byte:
		n, err := strconv.ParseInt(string(reply), 10, 0)
		return int(n), err
	case nil:
		return 0, ErrNil
	case Error:
		return 0, reply
	}
	return 0, fmt.Errorf("redisgo: unknow type of Int, get type %T", reply)
}

// Int64 is a helper that converts a command reply to 64 bit integer. If err is
// not equal to nil, then Int returns 0, err.
func Int64(reply interface{}, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	switch reply := reply.(type) {
	case int64:
		return reply, nil
	case int:
		return int64(reply), nil
	case []byte:
		n, err := strconv.ParseInt(string(reply), 10, 64)
		return n, err
	case nil:
		return 0, ErrNil
	case Error:
		return 0, reply
	}
	return 0, fmt.Errorf("redisgo: unknow type of Int64, get type %T", reply)
}

var errNegativeInt = errors.New("redisgo: unexpected value for Uint64")

// Uint64 is a helper that converts a command reply to 64 bit integer. If err is
// not equal to nil, then Int returns 0, err.
func Uint64(reply interface{}, err error) (uint64, error) {
	if err != nil {
		return 0, err
	}
	switch reply := reply.(type) {
	case int64:
		if reply < 0 {
			return 0, errNegativeInt
		}
		return uint64(reply), nil
	case []byte:
		n, err := strconv.ParseInt(string(reply), 10, 64)
		return uint64(n), err
	case nil:
		return 0, ErrNil
	case Error:
		return 0, reply
	}
	return 0, fmt.Errorf("redisgo: unknow type of Uint64, get type %T", reply)
}


// Float64 is a helper that converts a command reply to 64 bit float. If err is
// not equal to nil, then Float64 returns 0, err.
func Float64(reply interface{}, err error) (float64, error) {
	if err != nil {
		return 0, err
	}
	switch reply := reply.(type) {
	case []byte:
		n, err := strconv.ParseFloat(string(reply), 64)
		return n, err
	case nil:
		return 0, ErrNil
	case Error:
		return 0, reply
	}
	return 0, fmt.Errorf("redisgo: unknow type of Float64, get type %T", reply)
}

// String is a helper that converts a command reply to a string. If err is not
// equal to nil, then String returns "", err.
func String(reply interface{}, err error) (string, error) {
	if err != nil {
		return "", err
	}
	switch reply := reply.(type) {
	case string:
		return reply, nil
	case []byte:
		return string(reply), nil
	case nil:
		return "", ErrNil
	case Error:
		return "", reply
	}
	return "", fmt.Errorf("redisgo: unknow type of String, get type %T", reply)
}

// Bytes is a helper that converts a command reply to a slice of bytes. If err
// is not equal to nil, then Bytes returns nil, err.
func Bytes(reply interface{}, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	switch reply := reply.(type) {
	case []byte:
		return reply, nil
	case string:
		return []byte(reply), nil
	case nil:
		return nil, ErrNil
	case Error:
		return nil, reply
	}
	return nil, fmt.Errorf("redigo: unexpected type for Bytes, got type %T", reply)
}

// Bool is a helper that converts a command reply to a boolean. If err is not
// equal to nil, then Bool returns false, err.
func Bool(reply interface{}, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	switch reply := reply.(type) {
	case int64:
		return reply != 0, nil
	case []byte:
		return strconv.ParseBool(string(reply))
	case nil:
		return false, ErrNil
	case Error:
		return false, reply
	}
	return false, fmt.Errorf("redigo: unexpected type for Bool, got type %T", reply)
}

// Interfaces is a helper that converts a command reply to a simple array.if err is
// not nil, then return nil, err
func Interfaces(reply interface{}, err error) ([]interface{}, error) {
	if err != nil {
		return nil, err
	}
	switch reply := reply.(type) {
	case []interface{}:
		return reply, nil
	case nil:
		return nil, ErrNil
	case Error:
		return nil, reply
	}
	return nil, fmt.Errorf("redigo: unexpected type for array, got type %T", reply)
}

func sliceHelper(reply interface{}, err error, name string, makeSlice func(int), assign func(int, interface{}) error) error {
	if err != nil {
		return err
	}
	switch reply := reply.(type) {
	case []interface{}:
		makeSlice(len(reply))
		for i := range reply {
			if reply[i] != nil {
				if err := assign(i, reply[i]); err != nil {
					return err
				}
			}
		}
		return nil
	case nil:
		return ErrNil
	case Error:
		return reply
	}
	return fmt.Errorf("redisgo: unexpected type for %s, got type %T", name, reply)
}


// Float64s is a helper that converts an array command reply to a []float64. If
// err is not equal to nil, then Float64s returns nil, err.
func Float64s(reply interface{}, err error) ([]float64, error) {
	var result []float64
	err = sliceHelper(reply, err, "Float64s", func(n int) { result = make([]float64, n) }, func (i int, v interface{}) error {
		p, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("redisgo: unexpected element type for Floats64, got type %T", v)
		}
		f, err := strconv.ParseFloat(string(p), 64)
		if err != nil {
			return nil
		}
		result[i] = f
		return err
	})
	return result, err
}

// Strings is a helper that converts an array command reply to a []string. If
// err is not equal to nil, then Strings returns nil, err.
func Strings(reply interface{}, err error) ([]string, error) {
	var result []string
	err = sliceHelper(reply, err, "Strings", func(n int) { result = make([]string, n) }, func(i int, v interface{}) error {
		switch res := v.(type) {
		case string:
			result[i] = res
			return nil
		case []byte:
			result[i] = string(res)
			return nil
		default:
			return fmt.Errorf("redisgo: unexpected element type for Strings, got type %T", v)
		}
	})
	return result, err
}

// ByteSlices is a helper that converts an array command reply to a [][]byte.
// If err is not equal to nil, then ByteSlices returns nil, err. Nil array
// items are stay nil.
func ByteSlices(reply interface{}, err error) ([][]byte, error) {
	var result [][]byte
	err = sliceHelper(reply, err, "Bytes", func(n int) { result = make([][]byte, n) }, func(i int, v interface{}) error {
		p, ok := v.([]byte)
		if ok {
			return fmt.Errorf("redisgo: unexpected element type for ByteSlices, got type %T", v)
		}
		result[i] = p
		return err
	})
	return result, err
}

// Int64s is a helper that converts an array command reply to a []int64.
// If err is not equal to nil, then Int64s returns nil, err. Nil array
// items are stay nil.
func Int64s(reply interface{}, err error) ([]int64, error) {
	var result []int64
	err = sliceHelper(reply, err, "Int64s", func(n int) { result = make([]int64, n) }, func(i int, v interface{}) error {
		switch v := v.(type) {
		case int64:
			result[i] = v
			return nil
		case []byte:
			n, err := strconv.ParseInt(string(v), 10, 64)
			result[i] = n
			return err
		default:
			return fmt.Errorf("redisgo: unexpected element type for Int64s, got type %T", v)
		}
	})
	return result, err
}

// Ints is a helper that converts an array command reply to a []in.
// If err is not equal to nil, then Ints returns nil, err. Nil array
// items are stay nil.
func Ints(reply interface{}, err error) ([]int, error) {
	var result []int
	err = sliceHelper(reply, err, "Ints", func(n int) { result = make([]int, n) }, func(i int, v interface{}) error {
		switch v := v.(type) {
		case int64:
			n := int(v)
			if int64(n) != v {
				return strconv.ErrRange
			}
			result[i] = n
			return nil
		case []byte:
			n, err := strconv.Atoi(string(v))
			result[i] = n
			return err
		default:
			return fmt.Errorf("redisgo: unexpected element type for Ints, got type %T", v)
		}
	})
	return result, err
}


// StringMap is a helper that converts an array of strings (alternating key, value)
// into a map[string]string. The HGETALL and CONFIG GET commands return replies in this format.
func StringMap(reply interface{}, err error) (map[string]string, error) {
	values, err := Interfaces(reply, err)
	if err != nil {
		return nil, err
	}
	if len(values) % 2 != 0 {
		return nil, errors.New("redisgo: StringMap expects even number of values result")
	}
	m := make(map[string]string, len(values) / 2)
	for i := 0; i < len(values); i += 2 {
		key, keyOk := values[i].([]byte)
		val, valOk := values[i + 1].([]byte)
		if !keyOk || !valOk {
			return nil, errors.New("redisgo: StringMap key not a bulk string value")
		}
		m[string(key)] = string(val)
	}
	return m, nil
}

func IntMap(reply interface{}, err error) (map[string]int, error) {
	values, err := Interfaces(reply, err)
	if err != nil {
		return nil, err
	}
	if len(values) % 2 != 0 {
		return nil, errors.New("redisgo: StringMap expects even number of values result")
	}
	m := make(map[string]int, len(values) / 2)
	for i := 0; i < len(values); i += 2 {
		key, keyOk := values[i].([]byte)
		val, valOk := values[i + 1].([]byte)
		if !keyOk || !valOk {
			return nil, errors.New("redisgo: StringMap key not a bulk string value")
		}
		n, err := Int(val, nil)
		if err != nil {
			return nil, err
		}
		m[string(key)] = n
	}
	return m, nil
}

func Int64Map(reply interface{}, err error) (map[string]int64, error) {
	values, err := Interfaces(reply, err)
	if err != nil {
		return nil, err
	}
	if len(values) % 2 != 0 {
		return nil, errors.New("redisgo: StringMap expects even number of values result")
	}
	m := make(map[string]int64, len(values) / 2)
	for i := 0; i < len(values); i += 2 {
		key, keyOk := values[i].([]byte)
		val, valOk := values[i + 1].([]byte)
		if !keyOk || !valOk {
			return nil, errors.New("redisgo: StringMap key not a bulk string value")
		}
		n, err := Int64(val, nil)
		if err != nil {
			return nil, err
		}
		m[string(key)] = n
	}
	return m, nil
}