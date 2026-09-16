package response

import (
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// 字段必须导出,否则 json 序列化拿不到值,响应体会是空对象 {}。
type Response struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data any    `json:"data"`
}

func HandleSuccess(c *app.RequestContext, data any) {
	c.JSON(consts.StatusOK, Response{
		Code: SUCCESS_CODE,
		Msg:  SUCCESS_MSG,
		Data: data,
	})
}

func HandleParamsError(c *app.RequestContext) {
	c.JSON(consts.StatusOK, Response{
		Code: ERRORPARAMS_CODE,
		Msg:  ERRORPARAMS_MSG,
		Data: "",
	})
}

func HandleBussinessFail(c *app.RequestContext, msg string) {
	c.JSON(consts.StatusOK, Response{
		Code: ERRORBUSSINESS_CODE,
		Msg:  ERRORBUSSINESS_MSG + msg,
		Data: "",
	})
}
