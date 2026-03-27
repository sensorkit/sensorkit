import asyncio

import pytest
from pydantic import BaseModel

from sensorkit.backend.request import CallContext, ExtendedResponse, Request


@pytest.mark.asyncio
async def test_request(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    class RequestModel(BaseModel):
        foo: str

    class ResponseModel(BaseModel):
        val: int

    request = Request.define(
        name="foo",
        payload=RequestModel,
        response=ResponseModel,
    )

    async def foo_req(req: RequestModel):
        assert req.foo == "bar"
        return ResponseModel(val=42)

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(1.0):
        cli = kit.entity(sc.entity)

        call = cli.call(request, RequestModel(foo="bar"))
        response = await call.invoke()
        assert isinstance(response, ResponseModel)

        await call.wait()
        result = call.result()
        assert isinstance(result, ResponseModel) and result.val == 42


@pytest.mark.asyncio
async def test_extended_request(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    class RequestModel(BaseModel):
        foo: str

    class ResponseModel(ExtendedResponse):
        pass

    class ResultModel(BaseModel):
        val: int

    request = Request.define(
        name="foo",
        payload=RequestModel,
        response=ResponseModel,
        result=ResultModel,
    )

    async def foo_req(req: RequestModel, call: CallContext[ResponseModel, ResultModel]):
        assert req.foo == "bar"
        call.accept(response=ResponseModel())
        await call.succeed(result=ResultModel(val=42))

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(1.0):
        cli = kit.entity(sc.entity)

        call = cli.call(request, RequestModel(foo="bar"))
        response = await call.invoke()
        assert isinstance(response, ResponseModel) and response.call_state == "running"

        await call.wait()
        result = call.result()
        assert isinstance(result, ResultModel) and result.val == 42

