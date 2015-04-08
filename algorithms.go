// Heavily copied from https://github.com/datastream/skyline

package main

import (
	"math"
	"sort"
	"time"
)

func (ms Measurements) values() []float64 {
	var vals []float64
	for _, v := range ms {
		vals = append(vals, v.value)
	}
	return vals
}

func (ms Measurements) timestamps() []int64 {
	var vals []int64
	for _, v := range ms {
		vals = append(vals, v.timestamp)
	}
	return vals
}

func unDef(f float64) bool {
	if math.IsNaN(f) {
		return true
	}
	if math.IsInf(f, 1) {
		return true
	}
	if math.IsInf(f, -1) {
		return true
	}
	return false
}

func round(f float64, places int) float64 {
	shift := math.Pow(10, float64(places))
	return math.Floor(f*shift+0.5) / shift
}

// KS performs a Kolmogorov-Smirnov test for the two datasets, and returns the
// p-value for the null hypothesis that the two sets come from the same distribution.
func ks(data1, data2 []float64) float64 {

	n1, n2 := len(data1), len(data2)
	en1, en2 := float64(n1), float64(n2)

	var d float64
	var fn1, fn2 float64

	sort.Float64s(data1)
	sort.Float64s(data2)

	j1, j2 := 0, 0
	for j1 < n1 && j2 < n2 {
		d1 := data1[j1]
		d2 := data2[j2]

		if d1 <= d2 {
			for j1 < n1 && d1 == data1[j1] {
				j1++
				fn1 = float64(j1) / en1
			}
		}

		if d2 <= d1 {
			for j2 < n2 && d2 == data2[j2] {
				j2++
				fn2 = float64(j2) / en2
			}
		}

		if dt := math.Abs(fn2 - fn1); dt > d {
			d = dt
		}

	}
	en := math.Sqrt((en1 * en2) / (en1 + en2))
	return qks(en * d)
}

func qks(z float64) float64 {

	if z < 0. {
		panic("bad z in qks")
	}

	if z == 0. {
		return 1.
	}
	if z < 1.18 {
		return 1. - pks(z)
	}
	x := math.Exp(-2. * (z * z))
	return 2. * (x - math.Pow(x, 4) + math.Pow(x, 9))
}

func pks(z float64) float64 {

	if z < 0. {
		panic("bad z in KSdist")
	}
	if z == 0. {
		return 0.
	}
	if z < 1.18 {
		y := math.Exp(-1.23370055013616983 / (z * z))
		return 2.25675833419102515 * math.Sqrt(-math.Log(y)) * (y + math.Pow(y, 9) + math.Pow(y, 25) + math.Pow(y, 49))
	}

	x := math.Exp(-2. * (z * z))
	return 1. - 2.*(x-math.Pow(x, 4)+math.Pow(x, 9))
}

// BUG(Adam Drake): Assumes unimodal but not checked, add dip test
func mean(a []float64) float64 {
	Len := len(a)
	if Len == 0 {
		return 0.0
	}
	var tot float64
	for _, val := range a {
		tot += val
	}
	return tot / float64(Len)
}

func median(a []float64) float64 {
	Len := len(a)
	if Len == 0 {
		return 0.0
	}
	var median float64
	sort.Float64s(a)
	lhs := (Len - 1) / 2
	rhs := Len / 2
	if lhs == rhs {
		median = a[lhs]
	} else {
		median = (a[lhs] + a[rhs]) / 2.0
	}
	return median
}

// BUG(Adam Drake): Assumes unimodal but not checked, add dip test

func variance(a []float64) float64 {
	return cov(a, a)
}

// BUG(Adam Drake): Assumes unimodal but not checked, add dip test
func std(a []float64) float64 {
	return math.Sqrt(variance(a))
}

// BUG(Adam Drake): Does it make sense to return 0.0 when samples are not paired?
func cov(a, b []float64) float64 {
	if len(a) == 0 || len(b) == 0 || len(a) != len(b) {
		return 0.0
	}
	var sum float64
	aMean := mean(a)
	bMean := mean(b)
	for i := 0; i < len(a); i++ {
		sum += (a[i] - aMean) * (b[i] - bMean)
	}
	return sum / float64(len(a)-1)

}

// LinearRegressionLSE least squares linear regression
// BUG(Adam Drake): Assumptions for using linear regression are not checked.  See http://people.duke.edu/~rnau/testing.htm
// OLS slope is defined as covariance(x, y)/variance(x) and intercept as mean(y)-slope*mean(x)
func linearRegressionLSE(ts Measurements) (float64, float64) {
	times := ts.timestamps()
	vals := ts.values()
	var floatTimes []float64
	for _, v := range times {
		floatTimes = append(floatTimes, float64(v))
	}

	beta := cov(floatTimes, vals) / variance(floatTimes)
	alpha := mean(vals) - beta*mean(floatTimes)
	return alpha, beta
}

// EwmStd Exponentially-weighted moving std
func ewmStd(series []float64, com float64) []float64 {
	m1st := ewma(series, com)
	var series2 []float64
	for _, val := range series {
		series2 = append(series2, val*val)
	}
	m2nd := ewma(series2, com)
	l := len(m1st)
	var result []float64
	for i := 0; i < l; i++ {
		t := m2nd[i] - math.Pow(m1st[i], 2)
		t *= (1.0 + 2.0*com) / (2.0 * com)
		result = append(result, math.Sqrt(t))
	}
	return result
}

func histogram(series []float64, bins int) ([]int, []float64) {
	var binEdges []float64
	var hist []int
	l := len(series)
	if l == 0 {
		return hist, binEdges
	}
	sort.Float64s(series)
	w := (series[l-1] - series[0]) / float64(bins)
	for i := 0; i < bins; i++ {
		binEdges = append(binEdges, w*float64(i)+series[0])
		if binEdges[len(binEdges)-1] >= series[l-1] {
			break
		}
	}
	binEdges = append(binEdges, w*float64(bins)+series[0])
	bl := len(binEdges)
	hist = make([]int, bl-1)
	for i := 0; i < bl-1; i++ {
		for _, val := range series {
			if val >= binEdges[i] && val < binEdges[i+1] {
				hist[i] += 1
				continue
			}
			if i == (bl-2) && val >= binEdges[i] && val <= binEdges[i+1] {
				hist[i] += 1
			}
		}
	}
	return hist, binEdges
}

// KS2Samp
func kS2Samp(data1, data2 []float64) (float64, float64) {
	sort.Float64s(data1)
	sort.Float64s(data2)
	n1 := len(data1)
	n2 := len(data2)
	var dataAll []float64
	dataAll = append(dataAll, data1...)
	dataAll = append(dataAll, data2...)
	index1 := searchsorted(data1, dataAll)
	index2 := searchsorted(data2, dataAll)
	var cdf1 []float64
	var cdf2 []float64
	for _, v := range index1 {
		cdf1 = append(cdf1, float64(v)/float64(n1))
	}
	for _, v := range index2 {
		cdf2 = append(cdf2, float64(v)/float64(n2))
	}
	d := 0.0
	for i := 0; i < len(cdf1); i++ {
		d = math.Max(d, math.Abs(cdf1[i]-cdf2[i]))
	}
	return d, ks(data1, data2)
}

//np.searchsorted
func searchsorted(array, values []float64) []int {
	var indexes []int
	for _, val := range values {
		indexes = append(indexes, location(array, val))
	}
	return indexes
}

func location(array []float64, key float64) int {
	i := 0
	size := len(array)
	for {
		mid := (i + size) / 2
		if i == size {
			break
		}
		if array[mid] < key {
			i = mid + 1
		} else {
			size = mid
		}
	}
	return i
}

func ewma(series []float64, com float64) []float64 {
	var cur float64
	var prev float64
	var oldw float64
	var adj float64
	N := len(series)
	ret := make([]float64, N)
	if N == 0 {
		return ret
	}
	oldw = com / (1 + com)
	adj = oldw
	ret[0] = series[0] / (1 + com)
	for i := 1; i < N; i++ {
		cur = series[i]
		prev = ret[i-1]
		if unDef(cur) {
			ret[i] = prev
		} else {
			if unDef(prev) {
				ret[i] = cur / (1 + com)
			} else {
				ret[i] = (com*prev + cur) / (1 + com)
			}
		}
	}
	for i := 0; i < N; i++ {
		cur = ret[i]
		if !math.IsNaN(cur) {
			ret[i] = ret[i] / (1. - adj)
			adj *= oldw
		} else {
			if i > 0 {
				ret[i] = ret[i-1]
			}
		}
	}
	return ret
}

// tailAvg is a utility function used to calculate the average of the last three
// datapoints in the series as a measure, instead of just the last datapoint.
// It reduces noise, but it also reduces sensitivity and increases the delay
// to detection.
func tailAvg(ts []float64) float64 {
	l := len(ts)
	if l == 0 {
		return 0
	}
	if l < 3 {
		return ts[l-1]
	}
	return (ts[l-1] + ts[l-2] + ts[l-3]) / 3
}

// medianAbsoluteDeviation function
// A timeseries is anomalous if the deviation of its latest datapoint with
// respect to the median is X times larger than the median of deviations.
func medianAbsoluteDeviation(ts []float64) bool {
	med := median(ts)
	var normalized []float64
	for _, val := range ts {
		normalized = append(normalized, math.Abs(val-med))
	}
	medianDeviation := median(normalized)
	if medianDeviation == 0 {
		return false
	}
	testStatistic := normalized[len(normalized)-1] / medianDeviation
	if testStatistic > 6 {
		return true
	}
	return false
}

// Grubbs score
// A timeseries is anomalous if the Z score is greater than the Grubb's score.
// BUG(Adam Drake): Assumes unimodal but not checked, add dip test.
/*
func grubbs(ts []float64) bool {
	stdDev := std(series)
	mean := mean(series)
	tailAverage := tailAvg(series)
	zScore := (tailAverage - mean) / stdDev
	lenSeries := len(series)
	threshold := dst.StudentsTQtlFor(float64(lenSeries-2), 1-0.05/float64(2*lenSeries))
	thresholdSquared := threshold * threshold
	grubbsScore := (float64(lenSeries-1) / math.Sqrt(float64(lenSeries))) * math.Sqrt(thresholdSquared/(float64(lenSeries-2)+thresholdSquared))
	return zScore > grubbsScore
}
*/

// FirstHourAverage function
// Calcuate the simple average over one hour, FULLDURATION seconds ago.
// A timeseries is anomalous if the average of the last three datapoints
// are outside of three standard deviations of this value.
// BUG(Adam Drake): Assumes unimodal but not checked.  Add dip test.
func firstHourAverage(timeseries Measurements, fullDuration int64) bool {
	var series []float64
	lastHourThreshold := time.Now().Unix() - (fullDuration - 3600)
	for _, val := range timeseries {
		if val.timestamp < lastHourThreshold {
			series = append(series, val.value)
		}
	}
	mean := mean(series)
	stdDev := std(series)
	t := tailAvg(timeseries.values())
	return math.Abs(t-mean) > 3*stdDev
}

// SimpleStddevFromMovingAverage function
// A timeseries is anomalous if the absolute value of the average of the latest
// three datapoint minus the moving average is greater than one standard
// deviation of the average. This does not exponentially weight the MA and so
// is better for detecting anomalies with respect to the entire series.
// BUG(Adam Drake): Assumes unimodal but not checked.  Add dip test.
func simpleStddevFromMovingAverage(ts []float64) bool {
	mean := mean(ts)
	stdDev := std(ts)
	t := tailAvg(ts)
	return math.Abs(t-mean) > 3*stdDev
}

// StddevFromMovingAverage function
// A timeseries is anomalous if the absolute value of the average of the latest
// three datapoint minus the moving average is greater than one standard
// deviation of the moving average. This is better for finding anomalies with
// respect to the short term trends.
func stddevFromMovingAverage(ts []float64) bool {
	expAverage := ewma(ts, 50)
	stdDev := ewmStd(ts, 50)
	return math.Abs(ts[len(ts)-1]-expAverage[len(expAverage)-1]) > (3 * stdDev[len(stdDev)-1])
}

// MeanSubtractionCumulation function
// A timeseries is anomalous if the value of the next datapoint in the
// series is farther than a standard deviation out in cumulative terms
// / after subtracting the mean from each data point.
//BUG(Adam Drake): Handle case where len(ts) == 0
func meanSubtractionCumulation(ts []float64) bool {
	mean := mean(ts[:len(ts)-1])
	for i, val := range ts {
		ts[i] = val - mean
	}
	stdDev := std(ts[:len(ts)-1])
	return math.Abs(ts[len(ts)-1]) > 3*stdDev
}

// LeastSquares function
// A timeseries is anomalous if the average of the last three datapoints
// on a projected least squares model is greater than three sigma.
func leastSquares(ts Measurements) bool {
	m, c := linearRegressionLSE(ts)
	var errs []float64
	for _, val := range ts {
		projected := m*float64(val.timestamp) + c
		errs = append(errs, val.value-projected)
	}
	l := len(errs)
	if l < 3 {
		return false
	}
	stdDev := std(errs)
	t := (errs[l-1] + errs[l-2] + errs[l-3]) / 3
	return math.Abs(t) > stdDev*3 && math.Trunc(stdDev) != 0 && math.Trunc(t) != 0
}

// HistogramBins function
// A timeseries is anomalous if the average of the last three datapoints falls
// into a histogram bin with less than 20 other datapoints (you'll need to tweak
// that number depending on your data)
// Returns: the size of the bin which contains the tailAvg. Smaller bin size
// means more anomalous.
func histogramBins(timeseries Measurements) bool {
	series := timeseries.values()
	t := tailAvg(series)
	hist, bins := histogram(series, 15)
	for i, v := range hist {
		if v <= 20 {
			if i == 0 {
				if t <= bins[0] {
					return true
				}
			} else if t > bins[i] && t < bins[i+1] {
				return true
			}
		}
	}
	return false
}

// KsTest function
// A timeseries is anomalous if 2 sample Kolmogorov-Smirnov test indicates
// that data distribution for last 10 minutes is different from last hour.
// It produces false positives on non-stationary series so Augmented
// Dickey-Fuller test applied to check for stationarity.
func ksTest(timeseries Measurements) bool {
	current := time.Now().Unix()
	hourAgo := current - 3600
	tenMinutesAgo := current - 600
	var reference []float64
	var probe []float64
	for _, val := range timeseries {
		if val.timestamp >= hourAgo && val.timestamp < tenMinutesAgo {
			reference = append(reference, val.value)
		}
		if val.timestamp >= tenMinutesAgo {
			probe = append(probe, val.value)
		}
	}
	if len(reference) < 20 || len(probe) < 20 {
		return false
	}
	ksD, ksPValue := kS2Samp(reference, probe)
	if ksPValue < 0.05 && ksD > 0.5 {
		/*
			adf := ADFuller(reference, 10)
			if adf[1] < 0.05 {
				return true
			}
		*/
	}
	return false
}

// IsAnomalouslyAnomalous function
// This method runs a meta-analysis on the metric to determine whether the
// metric has a past history of triggering. TODO: weight intervals based on datapoint
func isAnomalouslyAnomalous(trigger_history Measurements, new_trigger Measurement) (bool, Measurements) {
	if len(trigger_history) == 0 {
		trigger_history = append(trigger_history, new_trigger)
		return true, trigger_history
	}
	if (new_trigger.value == trigger_history[len(trigger_history)-1].value) && (new_trigger.timestamp-trigger_history[len(trigger_history)-1].timestamp <= 300) {
		return false, trigger_history
	}
	trigger_history = append(trigger_history, new_trigger)
	trigger_times := trigger_history.timestamps()
	var intervals []float64
	for i := range trigger_times {
		if (i + 1) < len(trigger_times) {
			intervals = append(intervals, float64(trigger_times[i+1]-trigger_times[i]))
		}
	}
	mean := mean(intervals)
	stdDev := std(intervals)
	return math.Abs(intervals[len(intervals)-1]-mean) > 3*stdDev, trigger_history
}
