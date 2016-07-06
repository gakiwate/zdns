package zdns

import (
	"flag"
	_ "log"
	"strings"
)

/* Each lookup module registers a single GlobalLookupFactory, which is
 * instantiated once.  This global factory is responsible for providing command
 * line arguments and performing any configuration that should only occur once.
 * For each thread in the worker pool, the framework calls
 * MakePerRoutineFactory(), which should return a second factory, which should
 * perform any "per-thread" initialization. Within each "thread", the framework
 * will then call MakeLookup() for each connection it will make, on which it
 * will call DoLookup().  While two layers of factories is a bit... obnoxious,
 * this allows each module to maintain global, per-thread, and per-connection
 * state.
 *
 * Each layer has access to one proceeding layer (e.g., RoutineLookupFactory
 * knows the GlobalLookupFactory, from which it was created. Therefore, modules
 * should refer to this configuration instead of copying all configuration
 * values for every connection. The Base structs implement these basic
 * pieces of functionality and should be inherited in most situations.
 */

// one Lookup per IP/name/connection ==========================================
//
type Lookup interface {
	Initialize(r *RoutineLookupFactory) error
	DoLookup(name string) (interface{}, Status, error)
}

type BaseLookup struct {
	routineFactory *RoutineLookupFactory
}

func (s BaseLookup) Initialize(r *RoutineLookupFactory) error {
	s.routineFactory = r
	return nil
}

// one RoutineLookupFactory per goroutine =====================================
//
type RoutineLookupFactory interface {
	Initialize(g *GlobalLookupFactory) error
	MakeLookup() (Lookup, error)
}

type BaseRoutineLookupFactory struct {
	globalFactory *GlobalLookupFactory
}

func (s BaseRoutineLookupFactory) Initialize(g *GlobalLookupFactory) error {
	s.globalFactory = g
	return nil
}

// one RoutineLookupFactory per execution =====================================
//
type GlobalLookupFactory interface {
	// expected to add any necessary commandline flags if being
	// run as a standalone scanner
	AddFlags(flags *flag.FlagSet) error
	// global initialization. Gets called once globally
	// This is called after command line flags have been parsed
	Initialize(conf *GlobalConf) error
	// We can't set variables on an interface, so write functions
	// that define any settings for the factory
	AllowStdIn() bool
	// Return a single scanner which will scan a single host
	MakeRoutineFactory() (RoutineLookupFactory, error)
}

type BaseGlobalLookupFactory struct {
	globalConf *GlobalConf
}

func (l BaseGlobalLookupFactory) Initialize(c *GlobalConf) error {
	l.globalConf = c
	return nil
}

func (s BaseGlobalLookupFactory) AllowStdIn() bool {
	return true
}

// keep a mapping from name to factory
var lookups map[string]GlobalLookupFactory

func RegisterLookup(name string, s GlobalLookupFactory) {
	if lookups == nil {
		lookups = make(map[string]GlobalLookupFactory, 100)
	}
	lookups[name] = s
}

func ValidlookupsString() string {

	valid := make([]string, len(lookups))
	for k, _ := range lookups {
		valid = append(valid, k)
		//log.Debug("loaded module:", k)
	}
	return strings.Join(valid, ", ")
}

func GetLookup(name string) *GlobalLookupFactory {

	factory, ok := lookups[name]
	if !ok {
		return nil
	}
	return &factory
}
