// idl/hello.thrift
namespace go hello.example

struct HelloReq {
    1: string Name (api.query="name"); // Add api annotations for easier parameter binding
}

struct HelloResp {
    1: string RespBody;
}


service HelloService {
    HelloResp HelloMethod(1: HelloReq request) (api.get="/hello");
}
