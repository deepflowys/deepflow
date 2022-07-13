/*
 * Copyright (c) 2022 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cache

import (
	"fmt"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("recorder.cache")

func dbQueryResourceFailed(resource string, err error) string {
	return fmt.Sprintf("db query %s failed: %v", resource, err)
}

func dbResourceByLcuuidNotFound(resource, lcuuid string) string {
	return fmt.Sprintf("db %s (lcuuid: %s) not found", resource, lcuuid)
}

func dbResourceByIDNotFound(resource string, id int) string {
	return fmt.Sprintf("db %s (id: %d) not found", resource, id)
}

func cacheLcuuidByIDNotFound(resource string, id int) string {
	return fmt.Sprintf("cache %s lcuuid (id: %d) not found", resource, id)
}

func cacheIDByLcuuidNotFound(resource string, lcuuid string) string {
	return fmt.Sprintf("cache %s id (lcuuid: %s) not found", resource, lcuuid)
}

func addDiffBase(resource string, detail interface{}) string {
	return fmt.Sprintf("cache diff base add %s (detail: %+v) success", resource, detail)
}

func updateDiffBase(resource string, detail interface{}) string {
	return fmt.Sprintf("cache diff base update %s (detail: %+v) success", resource, detail)
}

func deleteDiffBase(resource, lcuuid string) string {
	return fmt.Sprintf("cache diff base delete %s (lcuuid: %s) success", resource, lcuuid)
}

func refreshResource(resource string) string {
	return fmt.Sprintf("refresh %s", resource)
}
