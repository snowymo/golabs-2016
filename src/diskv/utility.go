package diskv

import "shardmaster"
import (
	"strconv"
)
import "bytes"
import "encoding/gob"

func (kv *DisKV) sameShard(sh1 [shardmaster.NShards]int64, sh2 [shardmaster.NShards]int64) bool {
	for i, s := range sh1 {
		if s != sh2[i] {
			return false
		}
	}
	return true
}

func CopyMapMM(dstMap map[int]map[int64]int, srcMap map[int]map[int64]int) {
	for sh, m := range srcMap {
		if len(dstMap[sh]) == 0 {
			dstMap[sh] = make(map[int64]int)
		}
		CopyMapII(dstMap[sh], m)
	}
}

func CopyMapMMSh(dstMap map[int]map[int64]int, srcMap map[int]map[int64]int, shards []int) {
	for sh, m := range srcMap {
		if contains(shards, sh) {
			if len(dstMap[sh]) == 0 {
				dstMap[sh] = make(map[int64]int)
			}
			CopyMapII(dstMap[sh], m)
		}
	}
}

func CopyMapSS(dstMap map[string]string, srcMap map[string]string) {
	//DPrintf("before cpy dst:%v\tsrc:%v\n", dstMap, srcMap)
	for k, v := range srcMap {
		dstMap[k] = v
	}
	//DPrintf("after cpy %v\n", dstMap)
}

func CopyMapIB(dstMap map[int]bool, srcMap map[int]bool) {
	//DPrintf("before cpy dst:%v\tsrc:%v\n", dstMap, srcMap)
	for k, v := range srcMap {
		dstMap[k] = v
	}
	//DPrintf("after cpy %v\n", dstMap)
}

func CopyUidMap(dstMap map[int]map[int64]int, srcMap map[string]string, shard int) {
	for k, v := range srcMap {
		valu, _ := strconv.ParseInt(v, 10, 32)
		key, _ := strconv.ParseInt(k, 10, 64)
		if len(dstMap[shard]) == 0 {
			dstMap[shard] = make(map[int64]int)
		}
		dstMap[shard][key] = int(valu)
	}
}

func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func CopyMapIISh(dstMap map[int64]int, srcMap map[int64]int, shards []int, u2sh map[int64]int) {
	for k, _ := range srcMap {
		sh := u2sh[k]
		if contains(shards, sh) {
			if _, ok := dstMap[k]; !ok {
				// if not exist means it is not the original uid source, then assign -1
				dstMap[k] = -1
			}
		}
	}
	//DPrintf("after cpy %v\n", dstMap)
}

func CopyMapII(dstMap map[int64]int, srcMap map[int64]int) {
	for k, _ := range srcMap {
		if _, ok := dstMap[k]; !ok {
			// if not exist means it is not the original uid source, then assign -1
			dstMap[k] = -1
		}

	}
	//DPrintf("after cpy %v\n", dstMap)
}

func (kv *DisKV) turnAppendtoPut(op string) string {
	if op == "Append" {
		return "Put"
	}
	return op
}

func contain(item int, arr []int) bool {
	for _, v := range arr {
		if v == item {
			return true
		}
	}
	return false
}

func CopyMapSSSh(dstMap map[string]string, srcMap map[string]string, shards []int) {
	//DPrintf("before cpy dst:%v\tsrc:%v\n", dstMap, srcMap)
	for k, v := range srcMap {
		if contain(key2shard(k), shards) {
			dstMap[k] = v
			DPrintf("assign key-%v to shard-%d with dst-%v\n", k, key2shard(k), dstMap[k])
		}
	}
	//DPrintf("after cpy %v\n", dstMap)
	DPrintf("\n")
}

func CopyLogCache(dstMap map[int]Op, srcMap map[string]string) {
	for k, v := range srcMap {
		key, _ := strconv.ParseInt(k, 10, 64)
		dstMap[int(key)] = dec(v)
	}
}

func CopyMapIO(dstMap map[int]Op, srcMap map[int]Op) {
	for k, v := range srcMap {
		dstMap[k] = v
	}
}

func (kv *DisKV) sameMap2(map1 map[int64][]int, map2 map[int64][]int) bool {
	for k, v := range map1 {
		for i, _ := range v {
			if v[i] != map2[k][i] {
				return false
			}
		}
	}
	for k, v := range map2 {
		for i, _ := range v {
			if v[i] != map1[k][i] {
				return false
			}
		}
	}
	return true
}

func (kv *DisKV) sameOp(op1 Op, op2 Op) bool {
	if (op1.ConfigNo == op2.ConfigNo) && (op1.Key == op2.Key) && (op1.Oper == op2.Oper) && (op1.Uid == op2.Uid) && (op1.Value == op2.Value) && kv.sameMap2(op1.GSmap, op2.GSmap) {
		return true
	} else {
		return false
	}
}

func enc(op Op) string {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(op.Oper)
	e.Encode(op.Key)
	e.Encode(op.Value)
	e.Encode(op.ConfigNo)
	e.Encode(op.GSmap)
	e.Encode(op.Uid)
	return string(w.Bytes())
}

// decode a string originally produced by enc() and
// return the original values.
func dec(buf string) Op {
	r := bytes.NewBuffer([]byte(buf))
	d := gob.NewDecoder(r)
	var op Op
	d.Decode(&op.Oper)
	d.Decode(&op.Key)
	d.Decode(&op.Value)
	d.Decode(&op.ConfigNo)
	d.Decode(&op.GSmap)
	d.Decode(&op.Uid)
	return op
}

// encode two regular values into a string
// that can be saved in a file.
func encMap(m map[int]bool) string {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(len(m))
	for k, v := range m {
		e.Encode(k)
		e.Encode(v)
	}
	DPrintf("encMap %v to %v\n", m, string(w.Bytes()))
	return string(w.Bytes())
}

// decode a string originally produced by enc() and
// return the original values.
func decMap(buf string) map[int]bool {
	m := make(map[int]bool)
	r := bytes.NewBuffer([]byte(buf))
	d := gob.NewDecoder(r)
	var lens int
	d.Decode(&lens)
	for i := 0; i < lens; i++ {
		var key int
		var value bool
		d.Decode(&key)
		d.Decode(&value)
		m[key] = value
	}
	DPrintf("decMap %v\n", m)
	return m
}
