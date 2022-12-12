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
package cmd

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zmap/zdns/internal/util"
	"github.com/zmap/zdns/pkg/zdns"
)

// alookupCmd represents the alookup command
var sentinelCmd = &cobra.Command{
	Use:   "sentinel",
	Short: "SOA, CNAME followed by NS and A record lookups that follow CNAME records",
	Long: `extensive will get the information that is typically desired, instead
	of just the information that exists in a single record. Specifically,
	extensive will do a nslookup followed by alookup and will follow CNAME
	records.`,
	Run: func(cmd *cobra.Command, args []string) {
		GC.Module = strings.ToUpper("sentinel")
		zdns.Run(GC, cmd.Flags(),
			&Timeout, &IterationTimeout,
			&Class_string, &Servers_string,
			&Config_file, &Localaddr_string,
			&Localif_string, &NanoSeconds, &ClientSubnet_string)
	},
}

func init() {
	rootCmd.AddCommand(sentinelCmd)
	util.BindFlags(sentinelCmd, viper.GetViper(), util.EnvPrefix)
}
