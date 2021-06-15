package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Data struct {
	Airline              string
	AirlineID            string
	SourceAirport        string
	SourceAirportID      string
	DestinationAirport   string
	DestinationAirportID string
	Codeshare            string
	Stops                int
	Equipment            string
}

var filename = "./k.dat"

func main() {
	// open the file
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	r := bufio.NewReader(f)
	// array of Data struct which basically represent each line in the file
	// making it of length 0 and size approximately 100 thousand, so that it doesn't end up copying the slice everytime
	// it runs out of capacity.
	var allData = make([]Data, 0, 100000)
	// waitgroup because reading asynchronously,
	// asynchronously here has no real advantage as we are still waiting for the read to be completed.
	// It's just because the function was written like that earlier.
	var wg = sync.WaitGroup{}
	wg.Add(1)
	// seed the allData array from the file
	go func() {
		for {
			l, _, err := r.ReadLine()
			if err == io.EOF {
				wg.Done()
				return
			}
			// handle csv(returned array should be of length 9).
			k := strings.Split(string(l), ",")
			// this specific line didn't have 9 values, so just skip this line.
			if len(k) != 9 {
				continue
			}
			stops, err := strconv.Atoi(k[7])
			if err != nil {
				log.Println("cannot convert to integer", k[7])
			}
			for i := 0; i < 9; i++ {
				if k[i] == `\N` {
					k[i] = ""
				}
			}
			// creating a record of type Data
			d := Data{k[0], k[1], k[2], k[3], k[4], k[5], k[6], stops, k[8]}
			// appending to that allData
			allData = append(allData, d)
		}
	}()
	wg.Wait()
	fmt.Println("top 3 airlines which cover the maximum cities are", top3airlinesMaxCities(allData))
	fmt.Println("top 3 airlines which have direct flight routes", top3Direct(allData))
	fmt.Println("top 10 cities serving most airlines", top10citiesMaxAirlines(allData))
}

// #1
func top3airlinesMaxCities(data []Data) PairList {
	// here we are creating a map of airline to set of cities
	// the cardinality of the set is going to be the number of cities the airline serves.
	var aircities = map[string]map[string]int{}
	for _, v := range data {
		if _, ok := aircities[v.Airline]; !ok {
			aircities[v.Airline] = make(map[string]int)
		}
		aircities[v.Airline][v.SourceAirport] = 1
		aircities[v.Airline][v.DestinationAirport] = 1
	}
	return topn(aircities, 3)
}

// #2
func top3Direct(data []Data) PairList {
	// here we are creating a map of cities to set of airlines.
	// to resolve the dst to src and src to dst.
	// We have chosen the string that is lesser and then concatenate the dst and src based on this check, and this is the key.
	// cardinality of the set returns the direct flight number for the src dst concat string.
	var aircities = map[string]map[string]int{}
	for _, v := range data {
		if v.Stops == 0 {
			src := v.SourceAirport
			dst := v.DestinationAirport
			var city = ""
			if src < dst {
				city = src + ":" + dst
			} else {
				city = dst + ":" + src
			}
			if _, ok := aircities[v.Airline]; !ok {
				aircities[v.Airline] = make(map[string]int)
			}
			aircities[v.Airline][city] = 1
		}
	}
	return topn(aircities, 3)
}

// #3
func top10citiesMaxAirlines(data []Data) PairList {
	// here we are creating a map of cities to set of airlines.
	// the cardinality of the set is going to be the number of airlines served by that city.
	var aircities = map[string]map[string]int{}
	for _, v := range data {
		if _, ok := aircities[v.SourceAirport]; !ok {
			aircities[v.SourceAirport] = make(map[string]int)
		}

		if _, ok := aircities[v.DestinationAirport]; !ok {
			aircities[v.DestinationAirport] = make(map[string]int)
		}

		aircities[v.DestinationAirport][v.Airline] = 1
		aircities[v.SourceAirport][v.Airline] = 1
	}
	return topn(aircities, 10)
}

// topn returns the top n elements from the map of map
// sorted on the basis of len(map[key]).
func topn(abcd map[string]map[string]int, n int) PairList {
	pl := make(PairList, len(abcd))
	i := 0

	for k, v := range abcd {
		pl[i] = Pair{k, len(v)}
		i++
	}
	sort.Sort(sort.Reverse(pl))
	if n > len(pl) {
		return pl
	}

	return pl[:n]
}

type Pair struct {
	Key   string
	Value int
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
