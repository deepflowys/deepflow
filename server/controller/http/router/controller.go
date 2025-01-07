/*
 * Copyright (c) 2024 Yunshan Networks
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

package router

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"

	"github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/controller/config"
	"github.com/deepflowio/deepflow/server/controller/election"
	httpcommon "github.com/deepflowio/deepflow/server/controller/http/common"
	. "github.com/deepflowio/deepflow/server/controller/http/router/common"
	"github.com/deepflowio/deepflow/server/controller/http/service"
	"github.com/deepflowio/deepflow/server/controller/model"
	"github.com/deepflowio/deepflow/server/controller/monitor"
)

type Controller struct {
	cfg *config.ControllerConfig
	cc  *monitor.ControllerCheck

	middlewares []gin.HandlerFunc
}

func NewController(cfg *config.ControllerConfig, cc *monitor.ControllerCheck) *Controller {
	return &Controller{cfg: cfg, cc: cc}
}

func (c *Controller) RegisterTo(e *gin.Engine) {
	adminRoutes := e.Group("/v1/controllers")
	adminRoutes.Use(AdminPermissionVerificationMiddleware())

	adminRoutes.GET("/:lcuuid/", getController)
	adminRoutes.GET("/", getControllers)
	adminRoutes.PATCH("/:lcuuid/", updateController(c.cc, c.cfg))
	adminRoutes.DELETE("/:lcuuid/", deleteController(c.cc, c.cfg))
}

func getController(c *gin.Context) {
	args := make(map[string]string)
	args["lcuuid"] = c.Param("lcuuid")
	orgID, _ := c.Get(common.HEADER_KEY_X_ORG_ID)
	data, err := service.GetControllers(orgID.(int), args)
	log.Errorf("%+v", data)
	if err != nil {
		err = fmt.Errorf("org id(%v), %s", orgID, err.Error())
	}
	JsonResponse(c, data, err)
}

func getControllers(c *gin.Context) {
	args := make(map[string]string)
	if value, ok := c.GetQuery("ip"); ok {
		args["ip"] = value
	}
	if value, ok := c.GetQuery("controller"); ok {
		args["name"] = value
	}
	if value, ok := c.GetQuery("name"); ok {
		args["name"] = value
	}
	if value, ok := c.GetQuery("analyzer"); ok {
		args["analyzer_name"] = value
	}
	if value, ok := c.GetQuery("analyzer_ip"); ok {
		args["analyzer_ip"] = value
	}
	if value, ok := c.GetQuery("vtap"); ok {
		args["vtap_name"] = value
	}
	if value, ok := c.GetQuery("region"); ok {
		args["region"] = value
	}

	orgID, _ := c.Get(common.HEADER_KEY_X_ORG_ID)
	data, err := service.GetControllers(orgID.(int), args)
	log.Errorf("%+v", data)
	if err != nil {
		err = fmt.Errorf("org id(%d), %s", orgID.(int), err.Error())
	}
	JsonResponse(c, data, err)
}

func updateController(m *monitor.ControllerCheck, cfg *config.ControllerConfig) gin.HandlerFunc {
	return gin.HandlerFunc(func(c *gin.Context) {
		var err error
		var controllerUpdate model.ControllerUpdate

		// 如果不是masterController，将请求转发至是masterController
		isMasterController, masterControllerIP, _ := election.IsMasterControllerAndReturnIP()
		if !isMasterController {
			ForwardMasterController(c, masterControllerIP, cfg.ListenPort)
			return
		}

		// 参数校验
		err = c.ShouldBindBodyWith(&controllerUpdate, binding.JSON)
		if err != nil {
			BadRequestResponse(c, httpcommon.INVALID_PARAMETERS, err.Error())
			return
		}

		// 接收参数
		// 避免struct会有默认值，这里转为map作为函数入参
		patchMap := map[string]interface{}{}
		c.ShouldBindBodyWith(&patchMap, binding.JSON)

		lcuuid := c.Param("lcuuid")
		orgID, _ := c.Get(common.HEADER_KEY_X_ORG_ID)
		data, err := service.UpdateController(orgID.(int), lcuuid, patchMap, m, cfg)
		if err != nil {
			err = fmt.Errorf("org id(%d), %s", orgID.(int), err.Error())
		}
		JsonResponse(c, data, err)
	})
}

func deleteController(m *monitor.ControllerCheck, cfg *config.ControllerConfig) gin.HandlerFunc {
	return gin.HandlerFunc(func(c *gin.Context) {
		// if not master controller，should forward to master controller
		isMasterController, masterControllerIP, _ := election.IsMasterControllerAndReturnIP()
		if !isMasterController {
			ForwardMasterController(c, masterControllerIP, cfg.ListenPort)
			return
		}

		lcuuid := c.Param("lcuuid")
		orgID, _ := c.Get(common.HEADER_KEY_X_ORG_ID)
		data, err := service.DeleteController(orgID.(int), lcuuid, m)
		if err != nil {
			err = fmt.Errorf("org id(%d), %s", orgID.(int), err.Error())
		}
		JsonResponse(c, data, err)
		return
	})
}
