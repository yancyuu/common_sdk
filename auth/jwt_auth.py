# -*- coding: utf-8 -*-

import datetime
import time
import jwt

from ..system import sys_env


class JwtAuth():

    def encode_token(self, user_info, expired=None) -> str:
        config = {}
        if expired == 0:
            config.update({'iss': sys_env.get_env('JWT_ISSUER')})
        elif isinstance(expired, int) and expired > 0:
            config.update({'exp': datetime.datetime.now() + datetime.timedelta(seconds=expired), 'iss': sys_env.get_env('JWT_ISSUER')})
        else:
            config.update({
                'exp': datetime.datetime.now() + datetime.timedelta(seconds=int(sys_env.get_env('TOKEN_EXPIRED', 24 * 60 * 60))),
                'iss': sys_env.get_env('JWT_ISSUER'),
            })
        return jwt.encode(dict(config, **{'data': user_info}), sys_env.get_env("JWT_TOKEN_SECRET"), algorithm='HS256')

    def decode_token(self, token) -> dict:
        return jwt.decode(token,
                          sys_env.get_env("JWT_TOKEN_SECRET"),
                          issuer=sys_env.get_env('JWT_ISSUER'),
                          algorithms=['HS256'],
                          options={'verify_exp': False})

    def check_token(self, token) -> bool:
        decode_token = self.decode_token(token)
        print(f"decode_token  {decode_token}")
        return decode_token.get('exp') is None or decode_token.get('exp') > int(time.time())

    def get_token_data(self, token, key=None):
        data = self.decode_token(token).get('data', {})
        if key:
            if isinstance(key, list):
                ret = {}
                for x in key:
                    ret.update({x: data.get(x)})
                return ret
            return data.get(key)
        return data


jwt_auth = JwtAuth()
