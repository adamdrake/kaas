package main

import (
	"testing"
)

func TestMedian(t *testing.T) {
	series := []float64{0.1, 1.2, 2.3, 3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9.01}
	if median(series) != 5.05 {
		t.Fatal("wrong median", median(series))
	}
}

func TestLinearRegressionLSE(t *testing.T) {
	var timeseries []Measurement
	for i := 0; i < 10; i++ {
		a := Measurement{
			timestamp: int64(i),
			value:     float64(i)*3.1 - 2.1,
		}
		timeseries = append(timeseries, a)
	}
	m, c := linearRegressionLSE(timeseries)
	if m != 3.1 || c != 2.1 {
		t.Fatal("wrong linearregressionlse", t)
	}
}

func TestEwma(t *testing.T) {
	series := []float64{0.1, 1.2, 2.3, 3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9.01}
	rst := []float64{0.09999999999999978, 0.6554455445544544, 1.214520977649978, 1.7772255876832508, 2.3435583786886025, 2.9135180706168184, 3.48710309969332, 4.064311618855566, 4.645141498269393, 5.121538107701817}
	rt := ewma(series, 50)
	for i, v := range rt {
		if v != rst[i] {
			t.Fatal("ewma error", t)
		}
	}
}
func TestEwmStd(t *testing.T) {
	series := []float64{0.1, 1.2, 2.3, 3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9.01}
	rst := []float64{4.9526750297502914e-09, 0.5527160659008843, 0.902537317532201, 1.2357653238068602, 1.5629953497356235, 1.8872927402148911, 2.209889422762198, 2.531374353771067, 2.8520607676954124, 3.0195071357543375}
	rt := ewmStd(series, 50)
	for i, v := range rt {
		if v != rst[i] {
			t.Fatal("ewma error", t)
		}
	}
}

func TestHistogram(t *testing.T) {
	series := []float64{0.1, 1.2, 2.3, 3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9.01}
	hist := []int{1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 0, 2}
	bin := []float64{0.1, 0.694, 1.288, 1.8820000000000001, 2.476, 3.07, 3.664, 4.257999999999999, 4.851999999999999, 5.446, 6.039999999999999, 6.6339999999999995, 7.228, 7.821999999999999, 8.415999999999999, 9.01}
	h, b := histogram(series, 15)
	for i, v := range h {
		if v != hist[i] {
			t.Fatal("ewma error", t)
		}
	}
	for i, v := range b {
		if v != bin[i] {
			t.Fatal("ewma error", t)
		}
	}
}

func TestKS2Samp(t *testing.T) {
	reference := []float64{0.1, 1.2, 2.3, 3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9.01, 1.2, 2, 4, 6, 9, 1, 22, 11, 19, 18.9, 11, 14}
	probe := []float64{0.4, 0.1, 1.3, 2.4, 6.5, 3.6, 5.7, 6.8, 8.9, 9, 9.1, 11.2, 1.2, 1.3, 14, 4, 5, 0.123, 9, 7, 8.1, 9.9, 2.1}
	ksD, ksPValue := kS2Samp(reference, probe)
	if ksD != 0.18577075098814222 || ksPValue != 0.789955481957006 {
		t.Fatal("ewma error", t)
	}
}
