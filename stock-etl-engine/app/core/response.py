def success_response(data=None, message="success"):
    return {"code": 0, "message": message, "data": data}


def error_response(code: int = 9999, message: str = "error", data=None):
    return {"code": code, "message": message, "data": data}
