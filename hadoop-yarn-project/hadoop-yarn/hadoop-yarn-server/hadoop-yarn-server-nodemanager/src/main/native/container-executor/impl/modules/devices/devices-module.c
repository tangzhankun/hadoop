/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "configuration.h"
#include "container-executor.h"
#include "utils/string-utils.h"
#include "modules/devices/devices-module.h"
#include "modules/cgroups/cgroups-operations.h"
#include "modules/common/module-configs.h"
#include "modules/common/constants.h"
#include "util.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>

#define EXCLUDED_DEVICES_OPTION "excluded_devices"
#define CONTAINER_ID_OPTION "container_id"
#define MAX_CONTAINER_ID_LEN 128

static const struct section* cfg_section;

static int internal_handle_devices_request(
    update_cgroups_parameters_function update_cgroups_parameters_func_p,
    char** devices_number_tokens,
    const char* container_id) {
  int return_code = 0;

  char** blacklist_numbers = NULL;
  char* blacklist_str = get_section_value(DEVICES_BLACKLIST_DEVICES_NUMBERS,
     cfg_section);
  // Get denied "major:minor" device numbers from cfg, if not set, means all minor
  // devices can be used by YARN
  if (blacklist_str != NULL) {
    blacklist_numbers = split_delimiter(blacklist_str, ",");
    if (NULL == blacklist_numbers) {
      fprintf(ERRORFILE,
          "Invalid value set for %s, value=%s\n",
          DEVICES_BLACKLIST_DEVICES_NUMBERS, blacklist_str);
      return_code = -1;
      goto cleanup;
    }
  }

  const char** iterator = devices_number_tokens;
  int count = 0;
  // Check if any device number is in blacklist
  if (blacklist_numbers != NULL) {
    const char** blacklist_iterator = blacklist_numbers;
    int i = 0;
    while (iterator[count] != NULL) {
      i = 0;
      while (blacklist_iterator[i] != NULL) {
        if (strstr(iterator[count], blacklist_iterator[i]) != NULL) {
          // contains black-list device number
          fprintf(ERRORFILE, "Requested device not allowed: %s\n",
            iterator[count]);
          return_code = -1;
          goto cleanup;
        }
        i++;
      }
      count++;
    }
  }
  // Update valid cgroups devices values
  count = 0;
  while (iterator[count] != NULL) {
    int rc = update_cgroups_parameters_func_p("devices", "deny",
      container_id, iterator[count]);

    if (0 != rc) {
      fprintf(ERRORFILE, "CGroups: Failed to update cgroups\n");
      return_code = -1;
      goto cleanup;
    }
    count++;
  }

cleanup:
  if (blacklist_numbers != NULL) {
    free_values(blacklist_numbers);
  }
  if (devices_number_tokens != NULL) {
    free_values(devices_number_tokens);
  }

  return return_code;
}

void reload_devices_configuration() {
  cfg_section = get_configuration_section(DEVICES_MODULE_SECTION_NAME, get_cfg());
}

/*
 * Format of devices request commandline:
 * The excluded_devices is comma separated device cgroups values with device type.
 * c-e devices --excluded_devices b 8:16,c 244:0,c 244:1 --container_id container_x_y
 */
int handle_devices_request(update_cgroups_parameters_function func,
    const char* module_name, int module_argc, char** module_argv) {
  if (!cfg_section) {
    reload_devices_configuration();
  }

  if (!module_enabled(cfg_section, DEVICES_MODULE_SECTION_NAME)) {
    fprintf(ERRORFILE,
      "Please make sure devices module is enabled before using it.\n");
    return -1;
  }

  static struct option long_options[] = {
    {EXCLUDED_DEVICES_OPTION, required_argument, 0, 'e' },
    {CONTAINER_ID_OPTION, required_argument, 0, 'c' },
    {0, 0, 0, 0}
  };

  int c = 0;
  int option_index = 0;

  char** device_value_tokens = NULL;
  char container_id[MAX_CONTAINER_ID_LEN];
  memset(container_id, 0, sizeof(container_id));
  int failed = 0;

  optind = 1;
  while((c = getopt_long(module_argc, module_argv, "e:c:",
                         long_options, &option_index)) != -1) {
    switch(c) {
      case 'e':
        device_value_tokens = split_delimiter(optarg, ",");
        break;
      case 'c':
        if (!validate_container_id(optarg)) {
          fprintf(ERRORFILE,
            "Specified container_id=%s is invalid\n", optarg);
          failed = 1;
          goto cleanup;
        }
        strncpy(container_id, optarg, MAX_CONTAINER_ID_LEN);
        break;
      default:
        fprintf(ERRORFILE,
          "Unknown option in devices command character %d %c, optionindex = %d\n",
          c, c, optind);
        failed = 1;
        goto cleanup;
    }
  }

  if (0 == container_id[0]) {
    fprintf(ERRORFILE,
      "[%s] --container_id must be specified.\n", __func__);
    failed = 1;
    goto cleanup;
  }

  if (NULL == device_value_tokens) {
     // Devices number is null, skip following call.
     fprintf(ERRORFILE, "--excluded_devices is not specified, skip cgroups call.\n");
     goto cleanup;
  }

  failed = internal_handle_devices_request(func,
         device_value_tokens,
         container_id);

cleanup:
  if (device_value_tokens) {
    free_values(device_value_tokens);
  }
  return failed;
}
