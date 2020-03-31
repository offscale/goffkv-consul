package goffkv_consul

import (
    "strings"
    "time"
    "fmt"
    goffkv "github.com/offscale/goffkv"
    consulapi "github.com/hashicorp/consul/api"
)

const (
    ttl = "10s"
)

type consulClient struct {
    consul *consulapi.Client
    kv *consulapi.KV
    txn *consulapi.Txn
    prefixSegments []string
    sessionId string
    sessionRenewDoneCh chan struct{}
}

func makeQueryOptions() *consulapi.QueryOptions {
    return &consulapi.QueryOptions{
        RequireConsistent: true,
    }
}

func (c *consulClient) assemblePath(segments []string) string {
    parts := []string{}
    parts = append(parts, c.prefixSegments...)
    parts = append(parts, segments...)
    return strings.Join(parts, "/")
}

func New(address string, prefix string) (goffkv.Client, error) {
    prefixSegments, err := goffkv.DisassemblePath(prefix)
    if err != nil {
        return nil, err
    }
    config := consulapi.DefaultConfig()
    config.Address = address
    consul, err := consulapi.NewClient(config)
    if err != nil {
        return nil, err
    }
    return &consulClient{
        consul: consul,
        kv: consul.KV(),
        txn: consul.Txn(),
        prefixSegments: prefixSegments,
        sessionId: "",
        sessionRenewDoneCh: nil,
    }, nil
}

func (c *consulClient) maybeGetParent(segments []string) []*consulapi.TxnOp {
    if len(segments) > 1 {
        return []*consulapi.TxnOp{
            &consulapi.TxnOp{
                KV: &consulapi.KVTxnOp{
                    Verb: consulapi.KVGet,
                    Key: c.assemblePath(segments[:len(segments) - 1]),
                },
            },
        }
    }
    return []*consulapi.TxnOp{}
}

func (c *consulClient) getOrCreateSessionId() (string, error) {
    if c.sessionId != "" {
        return c.sessionId, nil
    }

    session := c.consul.Session()

    se := consulapi.SessionEntry{
        LockDelay: time.Nanosecond,
        Behavior: consulapi.SessionBehaviorDelete,
        TTL: ttl,
    }
    sessionId, _, err := session.Create(&se, nil)
    if err != nil {
        return "", err
    }

    c.sessionRenewDoneCh = make(chan struct{})
    go session.RenewPeriodic(ttl, sessionId, nil, c.sessionRenewDoneCh)

    c.sessionId = sessionId
    return sessionId, nil
}

func (c *consulClient) Create(key string, value []byte, lease bool) (goffkv.Version, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, err
    }

    checkParentOps := c.maybeGetParent(segments)

    ops := append(checkParentOps, &consulapi.TxnOp{
        KV: &consulapi.KVTxnOp{
            Verb: consulapi.KVCheckNotExists,
            Key: c.assemblePath(segments),
        },
    })

    if lease {
        sessionId, err := c.getOrCreateSessionId()
        if err != nil {
            return 0, err
        }
        ops = append(ops, &consulapi.TxnOp{
            KV: &consulapi.KVTxnOp{
                Verb: consulapi.KVLock,
                Key: c.assemblePath(segments),
                Value: value,
                Session: sessionId,
            },
        })
    } else {
        ops = append(ops, &consulapi.TxnOp{
            KV: &consulapi.KVTxnOp{
                Verb: consulapi.KVSet,
                Key: c.assemblePath(segments),
                Value: value,
            },
        })
    }

    ok, ret, _, err := c.txn.Txn(ops, makeQueryOptions())
    if err != nil {
        return 0, err
    }

    if ok {
        results := ret.Results
        lastResult := results[len(results) - 1]
        return lastResult.KV.ModifyIndex, nil
    } else {
        firstError := ret.Errors[0]
        switch {
        case firstError.OpIndex < len(checkParentOps):
            return 0, goffkv.OpErrNoEntry

        case firstError.OpIndex == len(checkParentOps):
            return 0, goffkv.OpErrEntryExists

        default:
            return 0, fmt.Errorf(
                "unexpected txn failure: %q at operation %d",
                firstError.What, firstError.OpIndex)
        }
    }
}

func (c *consulClient) Set(key string, value []byte) (goffkv.Version, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, err
    }

    ops := c.maybeGetParent(segments)
    ops = append(ops, &consulapi.TxnOp{
        KV: &consulapi.KVTxnOp{
            Verb: consulapi.KVSet,
            Key: c.assemblePath(segments),
            Value: value,
        },
    })

    ok, ret, _, err := c.txn.Txn(ops, makeQueryOptions())
    if err != nil {
        return 0, err
    }

    if ok {
        results := ret.Results
        lastResult := results[len(results) - 1]
        return lastResult.KV.ModifyIndex, nil
    } else {
        return 0, goffkv.OpErrNoEntry
    }
}

func (c *consulClient) Cas(key string, value []byte, ver goffkv.Version) (goffkv.Version, error) {
    if ver == 0 {
        resultVer, err := c.Create(key, value, false)
        if err == nil {
            return resultVer, nil
        }
        if err == goffkv.OpErrEntryExists {
            return 0, nil
        }
        return 0, err
    }

    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, err
    }

    ops := []*consulapi.TxnOp{
        &consulapi.TxnOp{
            KV: &consulapi.KVTxnOp{
                Verb: consulapi.KVGet,
                Key: c.assemblePath(segments),
            },
        },
        &consulapi.TxnOp{
            KV: &consulapi.KVTxnOp{
                Verb: consulapi.KVCAS,
                Key: c.assemblePath(segments),
                Value: value,
                Index: ver,
            },
        },
    }

    ok, ret, _, err := c.txn.Txn(ops, makeQueryOptions())
    if err != nil {
        return 0, err
    }

    if ok {
        results := ret.Results
        lastResult := results[len(results) - 1]
        return lastResult.KV.ModifyIndex, nil
    } else {
        firstError := ret.Errors[0]
        if firstError.OpIndex == 0 {
            return 0, goffkv.OpErrNoEntry
        }
        return 0, nil
    }
}

func (c *consulClient) Erase(key string, ver goffkv.Version) error {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return err
    }

    ops := []*consulapi.TxnOp{
        &consulapi.TxnOp{
            KV: &consulapi.KVTxnOp{
                Verb: consulapi.KVGet,
                Key: c.assemblePath(segments),
            },
        },
    }

    if ver == 0 {
        ops = append(ops, &consulapi.TxnOp{
            KV: &consulapi.KVTxnOp{
                Verb: consulapi.KVDelete,
                Key: c.assemblePath(segments),
            },
        })

    } else {
        ops = append(ops, &consulapi.TxnOp{
            KV: &consulapi.KVTxnOp{
                Verb: consulapi.KVDeleteCAS,
                Key: c.assemblePath(segments),
                Index: ver,
            },
        })
    }

    ops = append(ops, &consulapi.TxnOp{
        KV: &consulapi.KVTxnOp{
            Verb: consulapi.KVDeleteTree,
            Key: c.assemblePath(segments) + "/",
        },
    })

    ok, ret, _, err := c.txn.Txn(ops, makeQueryOptions())
    if err != nil {
        return err
    }

    if !ok {
        firstError := ret.Errors[0]
        if firstError.OpIndex == 0 {
            return goffkv.OpErrNoEntry
        }
        // else we do not need to return any error
    }
    return nil
}

func (c *consulClient) Exists(key string, watch bool) (goffkv.Version, goffkv.Watch, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, nil, err
    }

    frozenPath := c.assemblePath(segments)

    kv, _, err := c.kv.Get(frozenPath, makeQueryOptions())
    if err != nil {
        return 0, nil, err
    }

    if kv == nil {
        return 0, nil, nil
    }

    resultVer := kv.ModifyIndex
    var resultWatch goffkv.Watch

    if watch {
        resultWatch = func() {
            opts := makeQueryOptions()
            opts.WaitIndex = resultVer
            _, _, _ = c.kv.Get(frozenPath, opts)
        }
    }
    return resultVer, resultWatch, nil
}

func (c *consulClient) Get(key string, watch bool) (goffkv.Version, []byte, goffkv.Watch, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, nil, nil, err
    }

    frozenPath := c.assemblePath(segments)

    kv, _, err := c.kv.Get(frozenPath, makeQueryOptions())
    if err != nil {
        return 0, nil, nil, err
    }

    if kv == nil {
        return 0, nil, nil, goffkv.OpErrNoEntry
    }

    resultVer := kv.ModifyIndex
    var resultWatch goffkv.Watch

    if watch {
        resultWatch = func() {
            opts := makeQueryOptions()
            opts.WaitIndex = resultVer
            _, _, _ = c.kv.Get(frozenPath, opts)
        }
    }
    return resultVer, kv.Value, resultWatch, nil
}

func detachChild(path string, nPrefix int, nGlobalPrefix int) string {
    lastSlash := strings.LastIndexByte(path, '/')
    if lastSlash >= nPrefix {
        return ""
    }

    if nGlobalPrefix == 0 {
        return "/" + path
    } else {
        return path[nGlobalPrefix:]
    }
}

func (c *consulClient) Children(key string, watch bool) ([]string, goffkv.Watch, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return nil, nil, err
    }

    frozenPath := c.assemblePath(segments)
    frozenPrefix := frozenPath + "/"

    ops := []*consulapi.TxnOp{
        &consulapi.TxnOp{
            KV: &consulapi.KVTxnOp{
                Verb: consulapi.KVGetTree,
                Key: frozenPrefix,
            },
        },
        &consulapi.TxnOp{
            KV: &consulapi.KVTxnOp{
                Verb: consulapi.KVGet,
                Key: frozenPath,
            },
        },
    }

    ok, ret, _, err := c.txn.Txn(ops, makeQueryOptions())
    if err != nil {
        return nil, nil, err
    }

    if !ok {
        return nil, nil, goffkv.OpErrNoEntry
    }

    results := ret.Results

    globalPrefixLength := len(c.assemblePath([]string{}))
    children := []string{}
    for i := 0; i < len(results) - 1; i++ {
        child := detachChild(results[i].KV.Key, len(frozenPrefix), globalPrefixLength)
        if child != "" {
            children = append(children, child)
        }
    }

    var resultWatch goffkv.Watch
    if watch {
        var ver uint64
        for _, result := range results {
            curVer := result.KV.ModifyIndex
            if curVer > ver {
                ver = curVer
            }
        }
        resultWatch = func() {
            opts := makeQueryOptions()
            opts.WaitIndex = ver
            _, _, _ = c.kv.List(frozenPath, opts)
        }
    }

    return children, resultWatch, nil
}

type resultKind int
const (
    rkCreate resultKind = iota
    rkSet
    rkAux
)

func toUserOpIndex(boundaries []int, op int) int {
    for i, x := range boundaries {
        if x >= op {
            return i
        }
    }
    return -1
}

func (c *consulClient) Commit(txn goffkv.Txn) ([]goffkv.TxnOpResult, error) {
    ops := []*consulapi.TxnOp{}
    boundaries := []int{}
    rks := []resultKind{}

    for _, check := range txn.Checks {
        segments, err := goffkv.DisassembleKey(check.Key)
        if err != nil {
            return nil, err
        }

        if check.Ver == 0 {
            ops = append(ops, &consulapi.TxnOp{
                KV: &consulapi.KVTxnOp{
                    Verb: consulapi.KVGet,
                    Key: c.assemblePath(segments),
                },
            })
        } else {
            ops = append(ops, &consulapi.TxnOp{
                KV: &consulapi.KVTxnOp{
                    Verb: consulapi.KVCheckIndex,
                    Key: c.assemblePath(segments),
                    Index: check.Ver,
                },
            })
        }

        boundaries = append(boundaries, len(ops) - 1)
        rks = append(rks, rkAux)
    }

    for _, op := range txn.Ops {
        segments, err := goffkv.DisassembleKey(op.Key)
        if err != nil {
            return nil, err
        }

        switch op.What {
        case goffkv.Create:
            getParentOp := c.maybeGetParent(segments)
            ops = append(ops, getParentOp...)
            for _, _ = range getParentOp {
                rks = append(rks, rkAux)
            }

            ops = append(ops, &consulapi.TxnOp{
                KV: &consulapi.KVTxnOp{
                    Verb: consulapi.KVCheckNotExists,
                    Key: c.assemblePath(segments),
                },
            })
            // KVCheckNotExists does not produce any results

            if op.Lease {
                sessionId, err := c.getOrCreateSessionId()
                if err != nil {
                    return nil, err
                }
                ops = append(ops, &consulapi.TxnOp{
                    KV: &consulapi.KVTxnOp{
                        Verb: consulapi.KVLock,
                        Key: c.assemblePath(segments),
                        Value: op.Value,
                        Session: sessionId,
                    },
                })
                rks = append(rks, rkCreate)
            } else {
                ops = append(ops, &consulapi.TxnOp{
                    KV: &consulapi.KVTxnOp{
                        Verb: consulapi.KVSet,
                        Key: c.assemblePath(segments),
                        Value: op.Value,
                    },
                })
                rks = append(rks, rkCreate)
            }

        case goffkv.Set:
            ops = append(ops, &consulapi.TxnOp{
                KV: &consulapi.KVTxnOp{
                    Verb: consulapi.KVGet,
                    Key: c.assemblePath(segments),
                },
            })
            rks = append(rks, rkAux)
            ops = append(ops, &consulapi.TxnOp{
                KV: &consulapi.KVTxnOp{
                    Verb: consulapi.KVSet,
                    Key: c.assemblePath(segments),
                    Value: op.Value,
                },
            })
            rks = append(rks, rkSet)

        case goffkv.Erase:
            ops = append(ops, &consulapi.TxnOp{
                KV: &consulapi.KVTxnOp{
                    Verb: consulapi.KVGet,
                    Key: c.assemblePath(segments),
                },
            })
            rks = append(rks, rkAux)

            ops = append(ops, &consulapi.TxnOp{
                KV: &consulapi.KVTxnOp{
                    Verb: consulapi.KVDelete,
                    Key: c.assemblePath(segments),
                },
            })
            // KVDelete does not produce any results

            ops = append(ops, &consulapi.TxnOp{
                KV: &consulapi.KVTxnOp{
                    Verb: consulapi.KVDeleteTree,
                    Key: c.assemblePath(segments) + "/",
                },
            })
            // KVDeleteTree does not produce any results
        }

        boundaries = append(boundaries, len(ops) - 1)
    }

    ok, ret, _, err := c.txn.Txn(ops, makeQueryOptions())
    if err != nil {
        return nil, err
    }

    if ok {
        results := ret.Results
        answer := []goffkv.TxnOpResult{}
        for i := 0; i < len(results); i++ {
            ver := results[i].KV.ModifyIndex
            switch rks[i] {
            case rkCreate:
                answer = append(answer, goffkv.TxnOpResult{goffkv.Create, ver})
            case rkSet:
                answer = append(answer, goffkv.TxnOpResult{goffkv.Set, ver})
            }
        }
        return answer, nil

    } else {
        firstError := ret.Errors[0]
        userIndex := toUserOpIndex(boundaries, firstError.OpIndex)
        if userIndex < 0 {
            panic("txn failed on non-existing op")
        }
        return nil, goffkv.TxnError{userIndex}
    }
}

func (c *consulClient) Close() {
    if c.sessionRenewDoneCh != nil {
        close(c.sessionRenewDoneCh)
    }
}

func init() {
    goffkv.RegisterClient("consul", New)
}
