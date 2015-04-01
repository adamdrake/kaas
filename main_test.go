package main

import (
  "bytes"
  "testing"
)

func TestGraphiteEncoder(t *testing.T) {
  cs := []struct {
        ts int64
        val float64
        want []byte
  }{
    //                   fx[2] int   float float....................................value
    // python string x92d\xcb@Y\x00\x00\x00\x00\x00\x00
    {0x64, 100.0, []byte{0x92, 0x64, 0xcb, 0x40, 0x59, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},},
    {0x01, 1.0,   []byte{0x92, 0x01, 0xcb, 0x3F, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},},
  }
  e := newGraphiteEncoder()
  for _, c := range cs {
    e.buffer.Reset()
    e.encodeInt64Float64Tuple(c.ts, c.val)
    res := e.buffer.Bytes()
    if !bytes.Equal(res, c.want) {
      t.Fatal(res, c.want)
    }
  }
}
