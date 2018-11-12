//
// (c) 2017 by University of Delaware, Argonne National Laboratory, San Diego
//     Supercomputer Center, National University of Defense Technology,
//     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
//
//     See COPYRIGHT in top-level directory.
//

#if HAVE_PAPI

#include "log.h"
#include "stat.h"
#include "powerlimit.h"
#include "globals.h"
#include "mimir_config.h"

#define MAX_powercap_EVENTS 64

#include "papi.h"
int EventSet = PAPI_NULL;
long long oldvalues[MAX_powercap_EVENTS];
long long newvalues[MAX_powercap_EVENTS];
int limit_map[MAX_powercap_EVENTS];
int num_events = 0, num_limits = 0, retval = -1;
char event_names[MAX_powercap_EVENTS][PAPI_MAX_STR_LEN];

void init_power_limit()
{
    int cid, powercap_cid = -1, numcmp, r, code;
    const PAPI_component_info_t *cmpinfo = NULL;

    // Init PAPI
    retval = PAPI_library_init(PAPI_VER_CURRENT);
    if (retval != PAPI_VER_CURRENT) {
        LOG_ERROR("PAPI_library_init failed! ret=%d\n", retval);
    }

    // Find powercap component
    numcmp = PAPI_num_components();
    for (cid = 0; cid < numcmp; cid++) {
        if ((cmpinfo = PAPI_get_component_info(cid)) == NULL) {
            LOG_ERROR("PAPI_get_component_info failed\n");
        }
        if (strstr(cmpinfo->name, "powercap")) {
            powercap_cid = cid;
        }
        if (cmpinfo->disabled) {
            LOG_ERROR("powercap component disabled: %s\n",
                      cmpinfo->disabled_reason);
        }
    }
    if (cid == numcmp) {
        LOG_ERROR("No powercap component found\n");
    }

    // Add power events
    retval = PAPI_create_eventset(&EventSet);
    if (retval != PAPI_OK) {
        LOG_ERROR("PAPI_create_eventset error: ret=%d\n", retval);
    }
    code = PAPI_NATIVE_MASK;
    r = PAPI_enum_cmp_event(&code, PAPI_ENUM_FIRST, powercap_cid);
    while (r == PAPI_OK) {
        retval = PAPI_event_code_to_name(code, event_names[num_events]);
        if (retval != PAPI_OK) {
            LOG_ERROR("Error from PAPI_event_code_to_name! retval=%d\n",
                      retval);
        }
        retval = PAPI_add_event(EventSet, code);
        if (retval != PAPI_OK) break;
        if (!(strstr(event_names[num_events], "SUBZONE"))
            && (strstr(event_names[num_events], "POWER_LIMIT"))) {
            limit_map[num_limits] = num_events;
            num_limits++;
        }
        num_events++;
        r = PAPI_enum_cmp_event(&code, PAPI_ENUM_EVENTS, powercap_cid);
    }

    retval = PAPI_start(EventSet);
    if (retval != PAPI_OK) {
        LOG_ERROR("PAPI_start error!ret=%d\n", retval);
    }
    retval = PAPI_read(EventSet, oldvalues);
    if (retval != PAPI_OK) {
        LOG_ERROR("PAPI_read error!ret=%d\n", retval);
    }
}

void uinit_power_limit()
{
    retval = PAPI_stop(EventSet, oldvalues);
    if (retval != PAPI_OK) LOG_ERROR("PAPI_stop error!\n");

    retval = PAPI_cleanup_eventset(EventSet);
    if (retval != PAPI_OK) LOG_ERROR("PAPI_cleanup_eventset error!\n");

    retval = PAPI_destroy_eventset(&EventSet);
    if (retval != PAPI_OK) LOG_ERROR("PAPI_destroy_eventset error!\n");
}

void set_power_limit(double scale)
{
    for (int i = 0; i < num_events; i++) {
        newvalues[i] = oldvalues[i];
    }
    for (int i = 0; i < num_limits; i++) {
        newvalues[limit_map[i]] = (long long) (newvalues[limit_map[i]] * scale);
    }
    retval = PAPI_write(EventSet, newvalues);
    if (retval != PAPI_OK) {
        LOG_ERROR("PAPI_write error!\n");
    }
}

void print_power_limit()
{
    retval = PAPI_read(EventSet, newvalues);
    if (retval != PAPI_OK) {
        LOG_ERROR("PAPI_read error!\n");
    }
    for (int i = 0; i < num_events; i++) {
        fprintf(stdout, "EVENT: %s\tVALUE: %.02lf\n", event_names[i],
                (double) newvalues[i]);
    }
}

#endif
