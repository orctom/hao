import base64
import mimetypes
from io import BytesIO

import cv2
import numpy as np
import requests
from PIL import Image

import hao

LOGGER = hao.logs.get_logger(__name__)


def path_to_base64(path: str) -> str:
    match path:
        case path.startswith(('http', 'ftp')):
            with requests.get(path) as response:
                response.raise_for_status()
                return base64.b64encode(response.content).decode('utf-8')
        case _:
            with open(path, 'rb') as file:
                return base64.b64encode(file.read()).decode('utf-8')


def b64_to_pil(img_base64: str) -> Image:
    return Image.open(BytesIO(base64.b64decode(img_base64))).convert("RGB")


def b64_to_cv2(img_base64: str) -> np.ndarray:
    return cv2.imdecode(np.frombuffer(base64.b64decode(img_base64), np.uint8), cv2.IMREAD_COLOR)


def cv2_to_b64(img_cv2: np.ndarray) -> str:
    _, im_arr = cv2.imencode('.jpg', img_cv2)  # im_arr: image in Numpy one-dim array format.
    im_bytes = im_arr.tobytes()
    return base64.b64encode(im_bytes).decode('utf-8')


def cv2_to_pil(img_cv2: np.ndarray) -> Image:
    return Image.fromarray(img_cv2).convert("RGB")


def pil_to_b64(img_pil: Image) -> str:
    im_file = BytesIO()
    img_pil.save(im_file, format="JPEG")
    im_bytes = im_file.getvalue()
    return base64.b64encode(im_bytes).decode('utf-8')


def pil_to_cv2(img_pil: Image) -> np.ndarray:
    img_numpy = np.array(img_pil)
    return cv2.cvtColor(img_numpy, cv2.COLOR_RGB2BGR)


def resize_pil(img: Image, max_side=1568) -> Image:
    w, h = img.size
    if max(w, h) <= max_side:
        return img  # 已经够小，不处理
    scale = max_side / max(w, h)
    new_size = (int(w * scale), int(h * scale))
    return img.resize(new_size, Image.LANCZOS)


def resize_b64(img_b64: str, max_side=1568) -> str:
    return pil_to_b64(resize_pil(b64_to_pil(img_b64), max_side))


def resize_cv2(img_cv2: np.ndarray, max_side=1568) -> np.ndarray:
    return pil_to_cv2(resize_pil(cv2_to_pil(img_cv2), max_side))


def bytes_to_html(img_bytes: bytes) -> str:
    b64_encoded_image = base64.b64encode(img_bytes).decode()
    extention = mimetypes.guess_extension(img_bytes)
    img_type = extention[1:] if extention else 'png'
    return f"<img src='data:image/{img_type};base64,{b64_encoded_image}'>"
