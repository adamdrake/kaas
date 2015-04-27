package main

import (
  "bytes"
  "testing"
  "io"
)

type TestData struct {
  iVal int64
  fVal float64
  raw []byte
}

func data() []TestData{
  return []TestData{
    //                   fx[2] int   float float....................................value
    // python string x92d\xcb@Y\x00\x00\x00\x00\x00\x00
    {0x64, 100.0, []byte{0x92, 0x64, 0xcb, 0x40, 0x59, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},},
    {0x01, 1.0,   []byte{0x92, 0x01, 0xcb, 0x3F, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},},
  }
}

func TestGraphiteEncode(t *testing.T) {
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
func TestGraphiteDecode(t *testing.T) {
  cs := []struct {
        iTargets []int64
        fTargets []float64
        test []byte
  }{
    //                   fx[2] int   float float....................................value
    // python string x92d\xcb@Y\x00\x00\x00\x00\x00\x00
    {[]int64{0x64}, []float64{100.0}, []byte{0x92, 0x64, 0xcb, 0x40, 0x59, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},},
    {[]int64{0x01}, []float64{1.0}, []byte{0x92, 0x01, 0xcb, 0x3F, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},},
    {[]int64{0x01, 0x64}, []float64{1.0, 100.0},
       []byte{0x92, 0x01, 0xcb, 0x3F, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
              0x92, 0x64, 0xcb, 0x40, 0x59, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},},
  }
  for _, c := range cs {
    d := newGraphiteDecoder(bytes.NewReader(c.test))
    for i, iTarget := range c.iTargets {
      iVal, fVal, err := d.decodeInt64Float64Tuple()
      if iVal != iTarget {
        t.Fatal("Wrong integer, expected", iTarget, "got", iVal, "from bytes", c.test)
      }
      if fVal != c.fTargets[i] {
        t.Fatal("Wrong float, expected", c.fTargets[i], "got", fVal, "from bytes", c.test)
      }
      if err != nil {
        t.Fatal(err)
      }
    }
    _, _, err := d.decodeInt64Float64Tuple()
    if err != io.EOF {
      t.Fatal("Expected EOF")
    }
  }
}

func TestTrimMetrics(t *testing.T) {
  for _, c := range data() {
    res, _ := trimMetrics(1, bytes.NewReader(c.raw))
    if len(res) != 11 {
      t.Fatal("Got ", len(res), " instead of expected value 11.")
    }
  }
}
