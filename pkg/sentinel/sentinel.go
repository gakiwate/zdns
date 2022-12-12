/*
 * ZDNS Copyright 2016 Regents of the University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package extensive

import (
	"github.com/zmap/dns"
	"github.com/zmap/zdns/pkg/miekg"
	"github.com/zmap/zdns/pkg/zdns"

	"github.com/weppos/publicsuffix-go/publicsuffix"
)

// result to be returned by scan of host

type SentinelResult struct {
	Name     string `json:"name" groups:"short,normal,long,trace"`
	BaseName string `json:"base_name" groups:"short,normal,long,trace"`

	CNAMERecord   miekg.Result     `json:"cname" groups:"short,normal,long,trae"`
	SOARecord     miekg.Result     `json:"soa" groups:"short,normal,long,trace"`
	NameServers   []miekg.NSRecord `json:"nameservers" groups:"short,normal,long,trace"`
	IPv4Addresses []string         `json:"ipv4_addresses" groups:"short,normal,long,trace"`
	IPv6Addresses []string         `json:"ipv6_addresses" groups:"short,normal,long,trace"`
}

// Per Connection Lookup ======================================================
//
type Lookup struct {
	Factory *RoutineLookupFactory
	miekg.Lookup
}

// This LookupClient is created to call the actual implementation of DoMiekgLookup
type LookupClient struct{}

func (lc LookupClient) ProtocolLookup(s *miekg.Lookup, q miekg.Question, nameServer string) (interface{}, zdns.Trace, zdns.Status, error) {
	return s.DoMiekgLookup(q, nameServer)
}

func (s *Lookup) DoLookup(name, nameServer string) (interface{}, zdns.Trace, zdns.Status, error) {
	l := LookupClient{}
	lookupIpv4 := true
	lookupIpv6 := true

	retv := SentinelResult{}
	retv.Name = name

	res, trace, status, _ := s.DoMiekgLookup(miekg.Question{Name: name, Type: dns.TypeSOA}, nameServer)
	if status == zdns.STATUS_NOERROR {
		r, ok := res.(miekg.Result)
		if ok {
			retv.SOARecord = r
		}
	}

	rescn, tracecn, statuscn, _ := s.DoMiekgLookup(miekg.Question{Name: name, Type: dns.TypeCNAME}, nameServer)
	if statuscn == zdns.STATUS_NOERROR {
		r, ok := rescn.(miekg.Result)
		if ok {
			retv.CNAMERecord = r
		}
	}

	resns, tracens, statusns, _ := s.DoNSLookup(l, name, lookupIpv4, lookupIpv6, nameServer)
	if statusns == zdns.STATUS_NOERROR {
		retv.NameServers = resns.Servers
	}

	base_name, _ := publicsuffix.DomainFromListWithOptions(publicsuffix.DefaultList, name, &publicsuffix.FindOptions{IgnorePrivate: true})
	retv.BaseName = base_name

	res4, trace4, status4, _ := s.DoTargetedLookup(l, name, nameServer, true, false)
	if status4 == zdns.STATUS_NOERROR && res4 != nil {
		retv.IPv4Addresses = res4.(miekg.IpResult).IPv4Addresses
	}

	res6, trace6, status6, _ := s.DoTargetedLookup(l, name, nameServer, false, true)
	if status6 == zdns.STATUS_NOERROR && res6 != nil {
		retv.IPv6Addresses = res6.(miekg.IpResult).IPv6Addresses
	}

	trace = append(trace, tracens...)
	trace = append(trace, tracecn...)
	trace = append(trace, trace4...)
	trace = append(trace, trace6...)

	return retv, trace, zdns.STATUS_NOERROR, nil
}

// Per GoRoutine Factory ======================================================
//
type RoutineLookupFactory struct {
	miekg.RoutineLookupFactory
	Factory *GlobalLookupFactory
}

func (s *RoutineLookupFactory) MakeLookup() (zdns.Lookup, error) {
	a := Lookup{Factory: s}
	nameServer := s.Factory.RandomNameServer()
	a.Initialize(nameServer, dns.TypeNS, dns.ClassINET, &s.RoutineLookupFactory)
	return &a, nil
}

// Global Factory =============================================================
//
type GlobalLookupFactory struct {
	miekg.GlobalLookupFactory
}

// Command-line Help Documentation. This is the descriptive text what is
// returned when you run zdns module --help
func (s *GlobalLookupFactory) Help() string {
	return ""
}

func (s *GlobalLookupFactory) MakeRoutineFactory(threadID int) (zdns.RoutineLookupFactory, error) {
	r := new(RoutineLookupFactory)
	r.RoutineLookupFactory.Factory = &s.GlobalLookupFactory
	r.Factory = s
	r.ThreadID = threadID
	r.Initialize(s.GlobalConf)
	return r, nil
}

// Global Registration ========================================================
//
func init() {
	s := new(GlobalLookupFactory)
	zdns.RegisterLookup("SENTINEL", s)
}
