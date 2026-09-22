import asyncio
import json
import logging
import signal
import time

import httpx
import json_repair
import regex
from httpcore import ReadTimeout as HttpcoreReadTimeout
from httpcore import TimeoutException as HttpcoreTimeoutException
from httpx import TimeoutException as HttpxTimeoutException
from openai import (
    APIConnectionError,
    APIResponseValidationError,
    APIStatusError,
    AsyncOpenAI,
    BadRequestError,
    ContentFilterFinishReasonError,
    OpenAI,
)
from openai._types import omit
from openai.types.chat import ChatCompletionMessage
from openai.types.chat.parsed_chat_completion import ParsedChatCompletionMessage
from pydantic import ValidationError

from . import config

LOGGER = logging.getLogger(__name__)


class InvalidJsonError(Exception):
    """Invalid JSON"""


class PeriodicalChunksError(Exception):
    """大模型开始吐重复的输出"""


RETRY_EXCEPTIONS = (
    ValidationError,
    InvalidJsonError,
    APIResponseValidationError,
    APIStatusError,
    APIConnectionError,
    ContentFilterFinishReasonError,
)
TIMEOUT = httpx.Timeout(timeout=1800, connect=5.0)

P_NORM_JSON = regex.compile(r'(?:^\s*```(?:json|JSON)?\s*|\s*```\s*$)')


def has_periodical_pattern(arr: list[str], min_step: int = 1, max_step: int = 8, min_count: int = 50):
    """
    从后往前检测字符串数组是否存在周期性重复元素
    :param arr: 待测字符串数组
    :param min_step: 最小周期步长（周期单元长度）
    :param max_step: 最大周期步长
    :param min_count: 最少重复次数，默认50
    :return: bool 是否存在周期
    """
    n = len(arr)
    if n < min_step * min_count:
        return False

    # 边界校验
    min_step = max(1, min_step)
    max_step = min(max_step, n // min_count)
    if min_step > max_step:
        return False

    # 从后往前遍历，指针i是尾部当前基准位置
    # i 至少要留出 step * min_count 的空间向前校验
    for i in range(n - 1, (min_step * min_count) - 1, -1):
        # 尝试所有可能步长 [min_step, max_step]
        for step in range(min_step, max_step + 1):
            # 基准位置 i，对比 i-step 位置元素
            if arr[i] != arr[i - step]:
                continue

            # 找到第一个匹配，开始连续校验 min_count 组周期
            valid = True
            # 需要校验 min_count 次重复，每一组都要全部匹配
            for repeat_idx in range(1, min_count):
                offset = repeat_idx * step
                pos_curr = i - offset
                pos_prev = i - offset - step
                if pos_prev < 0:
                    valid = False
                    break
                if arr[pos_curr] != arr[pos_prev]:
                    valid = False
                    break
            if valid:
                LOGGER.info(f"[step] {step}")
                return True

    return False


class LLM():
    def __init__(self, profile: str = 'default'):
        super().__init__()
        conf = config.get(f"llm.{profile}", {})
        assert len(conf) > 0, f'llm profile not configured: llm.{self.profile}'
        self.model = conf.get('model')
        self.base_url = conf.get('base_url')
        self.api_key = conf.get('api_key', 'empty')
        self.extra_body = conf.get('extra_body')
        self.max_images = conf.get('max_images')
        LOGGER.debug(f"LLM: [{self.model}] ({self.base_url})")

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f"[{self.model}] {self.base_url}"

    @property
    def client(self) -> OpenAI:
        return OpenAI(api_key=self.api_key, base_url=self.base_url, timeout=TIMEOUT, max_retries=0)

    @property
    def async_client(self) -> AsyncOpenAI:
        return AsyncOpenAI(api_key=self.api_key, base_url=self.base_url, timeout=TIMEOUT, max_retries=0)

    def _completion(
        self,
        messages,
        max_tokens,
        tools=None,
        frequency_penalty: float | None = None,
        presence_penalty: float | None = None,
        thinking: bool = False,
        steps: bool | None = None,
        include_usage: bool = False,
        response_model: str | None = None,
    ):
        with self.client.chat.completions.stream(
            messages=messages,
            model=self.model,
            tools=tools or omit,
            max_completion_tokens=max_tokens,
            response_format=response_model or omit,
            temperature=.0,
            top_p=0.3,
            frequency_penalty=frequency_penalty or omit,
            presence_penalty=presence_penalty or omit,
            n=1,
            seed=0,
            stream_options={'include_usage': include_usage},
            extra_body={"chat_template_kwargs": {"enable_thinking": thinking}},
        ) as streams:
            i, chunks = 0, []
            for event in streams:
                if event.type == 'content.delta':
                    if len(chunks) > 200:
                        chunks.pop(0)
                    chunks.append(event.delta)
                    i += 1
                    if i >= 2000 and has_periodical_pattern(chunks):
                        raise PeriodicalChunksError()
                    if steps is True:
                        print(event.delta, flush=True, end='')
                    yield event.parsed if response_model else event.snapshot
                if event.type == 'content.done':
                    yield event.parsed if response_model else event.content
            completion = streams.get_final_completion()
            if completion.usage:
                LOGGER.info(f"[llm] tokens input: {completion.usage.prompt_tokens}, output: {completion.usage.completion_tokens}")
            return completion.choices[0].message

    def completion(
        self,
        messages,
        max_tokens,
        tools=None,
        thinking: bool = False,
        steps: bool | None = None,
        include_usage: bool = False,
        response_model: str | None = None,
        frequency_penalty: float | None = None,
        presence_penalty: float | None = None,
    ):
        gen = self._completion(
            messages,
            max_tokens,
            tools=tools,
            frequency_penalty=frequency_penalty,
            presence_penalty=presence_penalty,
            thinking=thinking,
            steps=steps,
            include_usage=include_usage,
            response_model=response_model,
        )
        try:
            max_len, count = 0, 0
            while True:
                val = next(gen)
                length = len(str(val))
                if length > max_len:
                    max_len, count = length, 0
                    continue
                count += 1
                if count < 1000:
                    continue
                try:
                    LOGGER.warning(f"[llm] stream snapshot not changing, returning the last snapshot\n--- snapshot begin ---\n{val}\n--- snapshot end ---")
                    if response_model:
                        return ParsedChatCompletionMessage(parsed=response_model.model_validate(val), role='assistant')
                    else:
                        return ChatCompletionMessage(content=val, role='assistant')
                except Exception as e:
                    LOGGER.exception(e)
                    raise e
        except StopIteration as e:
            return e.value

    def _build_messages(
        self,
        prompt: str,
        history: list[dict] | None = None,
        system: str | None = '',
        images: list[str] | None = None,
        functions: dict[str, callable] | None = None,
        max_words: int | None = None,
    ) -> list[dict]:
        if max_words:
            prompt = f"{prompt}\n\n字数控制在{max_words}字以内"
        content = [{"type": "text", "text": prompt}]
        if images:
            _images = images[:self.max_images] if self.max_images is not None and self.max_images > 0 else images
            for image_base64 in _images:
                content.append({
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{image_base64}"
                    },
                })

        if functions:
            system += f"\n仅在有需要的时候使用以下工具：{list(functions)}。不要调用列表之外的工具"
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        if history:
            messages.extend(history)
        messages.append({"role": "user", "content": content})
        return messages

    def _parse_result(self, msg, to_json: bool | None = None, response_model=None) -> str | dict | list:
        if response_model and msg.parsed:
            return msg.parsed

        text = msg.content
        if not to_json:
            return text
        try:
            return json_repair.loads(P_NORM_JSON.sub('', text))
        except Exception as e:
            LOGGER.error(f"invalid json: {repr(text)}")
            raise InvalidJsonError(e)

    def __call__(
        self,
        prompt: str,
        *,
        history: list[dict] | None = None,
        system: str | None = '',
        images: list[str] | None = None,
        to_json: bool | None = None,
        tools=None,
        functions: dict[str, callable] = None,
        max_tokens: int | None = None,
        max_words: int | None = None,
        thinking: bool = False,
        steps: bool | None = None,
        include_usage: bool = False,
        response_model: str | None = None,
        frequency_penalty: float | None = None,
        presence_penalty: float | None = None,
    ) -> str | dict | list:
        messages = self._build_messages(prompt, history, system, images, functions, max_words)

        def do_completion():
            return self.completion(
                messages,
                tools=tools,
                max_tokens=max_tokens,
                thinking=thinking,
                steps=steps,
                include_usage=include_usage,
                response_model=response_model,
                frequency_penalty=frequency_penalty,
                presence_penalty=presence_penalty,
            )

        msg = do_completion()
        if functions and msg.tool_calls:
            tool = msg.tool_calls[0]
            args = json.loads(tool.function.arguments)
            fn = functions.get(tool.function.name)
            if not fn:
                msg = do_completion()
            else:
                try:
                    fn_result = fn(**args)
                    messages.append({
                        'role': 'tool',
                        'tool_call_id': tool.id,
                        'name': tool.function.name,
                        'content': str(fn_result),
                    })
                    LOGGER.info(f"[llm] calling function: {tool.function.name} -> {fn_result}")
                    msg = do_completion()
                    LOGGER.info(f"[llm] function result: {msg.content}")
                except Exception as e:
                    LOGGER.error(e)
                    msg = do_completion()

        return self._parse_result(msg, to_json, response_model)

    def invoke(
        self,
        prompt: str,
        *,
        history: list[dict] | None = None,
        system: str | None = '',
        images: list[str] | None = None,
        to_json: bool | None = None,
        tools=None,
        functions: dict[str, callable] = None,
        max_tokens: int | None = None,
        max_words: int | None = None,
        thinking: bool = False,
        response_model: str | None = None,
        steps: bool = False,
        include_usage: bool = False,
        timeout: int = 600,
        retry_max: int = 3,
        retry_exceptions: tuple[Exception] | None = RETRY_EXCEPTIONS,
        retry_interval: int = 30,
        retry_delay: int = 30,
    ) -> str | dict | list:
        def handle(_, __):
            raise TimeoutError(f"[llm] timed out, retried {retry} times ({timeout}s each)")

        steps, frequency_penalty, presence_penalty = False, None, None
        for retry in range(retry_max + 1):
            if retry > 0:
                LOGGER.info(f"[llm] retry: {retry}, invoking...")

            try:
                old = signal.signal(signal.SIGALRM, handle)
                signal.alarm(timeout)
                wait = retry_interval + retry * retry_delay

                try:
                    return self(
                        prompt,
                        history=history,
                        system=system,
                        images=images,
                        to_json=to_json,
                        tools=tools,
                        functions=functions,
                        max_tokens=max_tokens,
                        max_words=max_words,
                        frequency_penalty=frequency_penalty,
                        presence_penalty=presence_penalty,
                        thinking=thinking,
                        steps=steps,
                        include_usage=include_usage,
                        response_model=response_model,
                    )

                except BadRequestError as e:
                    raise e
                except PeriodicalChunksError:
                    LOGGER.info(f"[llm] retried: {retry}, periodical chunks error, retry in {wait}s")
                    time.sleep(wait)
                except ValueError as e:
                    LOGGER.info(f"[llm] retried: {retry}, {e}")
                except (TimeoutError, HttpcoreReadTimeout, HttpcoreTimeoutException, HttpxTimeoutException) as e:
                    if retry == retry_max:
                        raise e
                    LOGGER.info(f"[llm] retried: {retry}, timeout ({timeout}s), retry in {wait}s")
                    time.sleep(wait)
                except Exception as e:
                    if retry == retry_max:
                        raise e
                    if retry_exceptions and not isinstance(e, retry_exceptions):
                        raise e
                    LOGGER.info(f"[llm] retried: {retry}, error: {e}, retry in {wait}s")
                    time.sleep(wait)
                finally:
                    steps, frequency_penalty, presence_penalty = True, 0, 0
                    signal.signal(signal.SIGALRM, old)
                    signal.alarm(0)
            except ValueError:
                LOGGER.warning("[llm] SIGALRM not supported in current thread, running without timeout")
                return self(
                    prompt,
                    history=history,
                    system=system,
                    images=images,
                    to_json=to_json,
                    tools=tools,
                    functions=functions,
                    max_tokens=max_tokens,
                    max_words=max_words,
                    frequency_penalty=frequency_penalty,
                    presence_penalty=presence_penalty,
                    thinking=thinking,
                    steps=steps,
                    include_usage=include_usage,
                    response_model=response_model,
                )

    async def _acompletion(
        self,
        messages,
        max_tokens,
        tools=None,
        frequency_penalty: float | None = None,
        presence_penalty: float | None = None,
        thinking: bool = False,
        steps: bool | None = None,
        include_usage: bool = False,
        response_model: str | None = None,
    ):
        async with self.async_client.chat.completions.stream(
            messages=messages,
            model=self.model,
            tools=tools or omit,
            max_completion_tokens=max_tokens,
            response_format=response_model or omit,
            temperature=.0,
            top_p=0.3,
            frequency_penalty=frequency_penalty or omit,
            presence_penalty=presence_penalty or omit,
            n=1,
            seed=0,
            stream_options={'include_usage': include_usage},
            extra_body={"chat_template_kwargs": {"enable_thinking": thinking}},
        ) as streams:
            i, chunks = 0, []
            async for event in streams:
                if event.type == 'content.delta':
                    if len(chunks) > 200:
                        chunks.pop(0)
                    chunks.append(event.delta)
                    i += 1
                    if i >= 2000 and has_periodical_pattern(chunks):
                        raise PeriodicalChunksError()
                    if steps is True:
                        print(event.delta, flush=True, end='')
                if event.type == 'content.done':
                    pass
            completion = await streams.get_final_completion()
            if completion.usage:
                LOGGER.info(f"[llm] tokens input: {completion.usage.prompt_tokens}, output: {completion.usage.completion_tokens}")
            yield completion.choices[0].message
            return

    async def acompletion(
        self,
        messages,
        max_tokens,
        tools=None,
        thinking: bool = False,
        steps: bool | None = None,
        include_usage: bool = False,
        response_model: str | None = None,
        frequency_penalty: float | None = None,
        presence_penalty: float | None = None,
    ):
        gen = self._acompletion(
            messages,
            max_tokens,
            tools=tools,
            frequency_penalty=frequency_penalty,
            presence_penalty=presence_penalty,
            thinking=thinking,
            steps=steps,
            include_usage=include_usage,
            response_model=response_model,
        )
        try:
            max_len, count = 0, 0
            async for val in gen:
                if isinstance(val, (ParsedChatCompletionMessage, ChatCompletionMessage)):
                    return val
                length = len(str(val))
                if length > max_len:
                    max_len, count = length, 0
                    continue
                count += 1
                if count < 1000:
                    continue
                try:
                    LOGGER.warning(f"[llm] stream snapshot not changing, returning the last snapshot\n--- snapshot begin ---\n{val}\n--- snapshot end ---")
                    if response_model:
                        return ParsedChatCompletionMessage(parsed=response_model.model_validate(val), role='assistant')
                    else:
                        return ChatCompletionMessage(content=val, role='assistant')
                except Exception as e:
                    LOGGER.exception(e)
                    raise e
        except StopAsyncIteration as e:
            return e.value

    async def __acall__(
        self,
        prompt: str,
        *,
        history: list[dict] | None = None,
        system: str | None = '',
        images: list[str] | None = None,
        to_json: bool | None = None,
        tools=None,
        functions: dict[str, callable] = None,
        max_tokens: int | None = None,
        max_words: int | None = None,
        thinking: bool = False,
        steps: bool | None = None,
        include_usage: bool = False,
        response_model: str | None = None,
        frequency_penalty: float | None = None,
        presence_penalty: float | None = None,
    ) -> str | dict | list:
        messages = self._build_messages(prompt, history, system, images, functions, max_words)

        async def do_completion():
            return await self.acompletion(
                messages,
                tools=tools,
                max_tokens=max_tokens,
                thinking=thinking,
                steps=steps,
                include_usage=include_usage,
                response_model=response_model,
                frequency_penalty=frequency_penalty,
                presence_penalty=presence_penalty,
            )

        msg = await do_completion()
        if functions and msg.tool_calls:
            tool = msg.tool_calls[0]
            args = json.loads(tool.function.arguments)
            fn = functions.get(tool.function.name)
            if not fn:
                msg = await do_completion()
            else:
                try:
                    fn_result = fn(**args)
                    messages.append({
                        'role': 'tool',
                        'tool_call_id': tool.id,
                        'name': tool.function.name,
                        'content': str(fn_result),
                    })
                    LOGGER.info(f"[llm] calling function: {tool.function.name} -> {fn_result}")
                    msg = await do_completion()
                    LOGGER.info(f"[llm] function result: {msg.content}")
                except Exception as e:
                    LOGGER.error(e)
                    msg = await do_completion()

        return self._parse_result(msg, to_json, response_model)

    async def ainvoke(
        self,
        prompt: str,
        *,
        history: list[dict] | None = None,
        system: str | None = '',
        images: list[str] | None = None,
        to_json: bool | None = None,
        tools=None,
        functions: dict[str, callable] = None,
        max_tokens: int | None = None,
        max_words: int | None = None,
        thinking: bool = False,
        response_model: str | None = None,
        steps: bool = False,
        include_usage: bool = False,
        timeout: int = 600,
        retry_max: int = 3,
        retry_exceptions: tuple[Exception] | None = RETRY_EXCEPTIONS,
        retry_interval: int = 30,
        retry_delay: int = 30,
    ) -> str | dict | list:
        steps, frequency_penalty, presence_penalty = False, None, None
        for retry in range(retry_max + 1):
            if retry > 0:
                LOGGER.info(f"[llm] retry: {retry}, invoking...")

            wait = retry_interval + retry * retry_delay

            try:
                return await asyncio.wait_for(
                    self.__acall__(
                        prompt,
                        history=history,
                        system=system,
                        images=images,
                        to_json=to_json,
                        tools=tools,
                        functions=functions,
                        max_tokens=max_tokens,
                        max_words=max_words,
                        frequency_penalty=frequency_penalty,
                        presence_penalty=presence_penalty,
                        thinking=thinking,
                        steps=steps,
                        include_usage=include_usage,
                        response_model=response_model,
                    ),
                    timeout=timeout,
                )

            except BadRequestError as e:
                raise e
            except PeriodicalChunksError:
                wait = 120
                LOGGER.info(f"[llm] retried: {retry}, periodical chunks error, retry in {wait}s")
                await asyncio.sleep(wait)
            except ValueError as e:
                LOGGER.info(f"[llm] retried: {retry}, {e}")
            except (TimeoutError, HttpcoreReadTimeout, HttpcoreTimeoutException, HttpxTimeoutException, asyncio.TimeoutError):
                if retry == retry_max:
                    raise TimeoutError(f"[llm] timed out, retried {retry} times ({timeout}s each)")
                LOGGER.info(f"[llm] retried: {retry}, timeout ({timeout}s), retry in {wait}s")
                await asyncio.sleep(wait)
            except Exception as e:
                if retry == retry_max:
                    raise e
                if retry_exceptions and not isinstance(e, retry_exceptions):
                    raise e
                LOGGER.info(f"[llm] retried: {retry}, error: {e}, retry in {wait}s")
                await asyncio.sleep(wait)
            finally:
                steps, frequency_penalty, presence_penalty = True, 0, 0
