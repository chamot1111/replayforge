package main

import (
	"encoding/json"
	"fmt"

	"log"

	lua "github.com/Shopify/go-lua"
	"github.com/vjeantet/grok"
)

var grokParser *grok.Grok

func init() {
	var err error
	grokParser, err = grok.NewWithConfig(&grok.Config{NamedCapturesOnly: true})
	if err != nil {
		log.Fatalf("Failed to initialize grok parser: %v", err)
	}
}

// RegisterLuaLibs registers custom Lua libraries and functions
func RegisterLuaLibs(vm *lua.State) {
	vm.Register("grok_parse", luaGrokParse)
	vm.Register("json_decode", luaJSONDecode)
	vm.Register("json_encode", luaJSONEncode)
}

func luaJSONDecode(l *lua.State) int {
	jsonStr, _ := l.ToString(-1)
	var result interface{}
	err := json.Unmarshal([]byte(jsonStr), &result)
	if err != nil {
		l.PushNil()
		l.PushString(err.Error())
		return 2
	}
	pushLuaValue(l, result)
	return 1
}

func luaJSONEncode(l *lua.State) int {
	value := getLuaValue(l, -1)
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		l.PushNil()
		l.PushString(err.Error())
		return 2
	}
	l.PushString(string(jsonBytes))
	return 1
}

func pushLuaValue(l *lua.State, value interface{}) {
	switch v := value.(type) {
	case nil:
		l.PushNil()
	case bool:
		l.PushBoolean(v)
	case float64:
		l.PushNumber(v)
	case string:
		l.PushString(v)
	case []interface{}:
		l.CreateTable(len(v), 0)
		for i, elem := range v {
			pushLuaValue(l, elem)
			l.RawSetInt(-2, i+1)
		}
	case map[string]interface{}:
		l.CreateTable(0, len(v))
		for key, elem := range v {
			l.PushString(key)
			pushLuaValue(l, elem)
			l.SetTable(-3)
		}
	default:
		l.PushString(fmt.Sprintf("%v", v))
	}
}
func getLuaValue(l *lua.State, index int) interface{} {
	luaType := l.TypeOf(index)
	switch luaType {
	case lua.TypeNil:
		return nil
	case lua.TypeBoolean:
		value := l.ToBoolean(index)
		return value
	case lua.TypeNumber:
		value := lua.CheckNumber(l, index)
		return value
	case lua.TypeString:
		value := lua.CheckString(l, index)
		return value
	case lua.TypeTable:
		return getLuaTable(l, index)
	default:
		return nil
	}
}

func getLuaTable(l *lua.State, index int) interface{} {
	if !l.IsTable(index) {
		return nil
	}

	if index < 0 {
		index = l.AbsIndex(index)
	}

	isArray := true
	arrayLen := 0
	result := make(map[string]interface{})

	l.PushNil()

	defer func() {
		if r := recover(); r != nil {
		}
	}()

	for l.Next(index) {
		key := getLuaValue(l, -2)
		value := getLuaValue(l, -1)

		if keyInt, ok := key.(float64); ok && isArray {
			if int(keyInt) != arrayLen+1 {
				isArray = false
			} else {
				arrayLen++
			}
		} else {
			isArray = false
		}

		if keyStr, ok := key.(string); ok {
			result[keyStr] = value
		} else {
			result[fmt.Sprintf("%v", key)] = value
		}

		l.Pop(1)
	}

	if isArray {
		array := make([]interface{}, arrayLen)
		for i := 0; i < arrayLen; i++ {
			array[i] = result[fmt.Sprintf("%d", i+1)]
		}
		return array
	}

	return result
}

func luaGrokParse(l *lua.State) int {
	if l.TypeOf(-2) != lua.TypeString || l.TypeOf(-1) != lua.TypeString {
		l.PushNil()
		l.PushString("Invalid arguments: expected (string, string)")
		return 2
	}

	pattern, _ := l.ToString(-2)
	text, _ := l.ToString(-1)
	values, err := grokParser.Parse(pattern, text)
	if err != nil {
		l.PushNil()
		l.PushString(err.Error())
		return 2
	}
	l.NewTable()
	for k, v := range values {
		l.PushString(k)
		l.PushString(v)
		l.SetTable(-3)
	}
	return 1
}
