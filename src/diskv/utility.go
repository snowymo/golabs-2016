package diskv

import "shardmaster"
import (
	"strconv"
)

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

func CopyUidMap(dstMap map[int]map[int64]int, srcMap map[string]string, shard int) {
	for k, v := range srcMap {
		valu, _ := strconv.ParseInt(v, 10, 32)
		key, _ := strconv.ParseInt(k, 10, 64)
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
