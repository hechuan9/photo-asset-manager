# /// script
# requires-python = ">=3.11"
# dependencies = ["PyJWT[crypto]>=2.10,<3"]
# ///
"""Read-only App Store Connect build status; credentials are injected by codex-secret."""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import jwt

APP_IDS = {"macos": "6816541220", "ios": "6816541067"}
API = "https://api.appstoreconnect.apple.com"


def make_token():
    names = ("ASC_KEY_ID", "ASC_ISSUER_ID", "ASC_PRIVATE_KEY")
    if any(not os.environ.get(name) for name in names):
        raise ValueError("必须配置 ASC_KEY_ID、ASC_ISSUER_ID、ASC_PRIVATE_KEY。")
    now = int(time.time())
    try:
        return jwt.encode(
            {"iss": os.environ["ASC_ISSUER_ID"], "iat": now, "exp": now + 600,
             "aud": "appstoreconnect-v1"},
            os.environ["ASC_PRIVATE_KEY"], algorithm="ES256",
            headers={"kid": os.environ["ASC_KEY_ID"], "typ": "JWT"},
        )
    except Exception:
        raise ValueError("无法使用 ASC_PRIVATE_KEY 签名，请检查 PKCS8 EC 私钥。") from None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def build_status(platform):
    query = urllib.parse.urlencode({
        "filter[app]": APP_IDS[platform], "sort": "-uploadedDate", "limit": "1",
        "include": "buildBetaDetail,preReleaseVersion,betaGroups",
    })
    request = urllib.request.Request(
        API + "/v1/builds?" + query,
        headers={"Authorization": "Bearer " + make_token(), "Accept": "application/json"},
    )
    try:
        with urllib.request.build_opener(NoRedirect).open(request, timeout=30) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"App Store Connect HTTP {error.code}") from None
    except urllib.error.URLError:
        raise RuntimeError("无法连接 App Store Connect。") from None
    included = {(item["type"], item["id"]): item["attributes"] for item in payload.get("included", [])}
    builds = []
    for item in payload["data"]:
        attributes = item["attributes"]
        result = {"id": item["id"], **{name: attributes.get(name) for name in
                  ("version", "uploadedDate", "processingState", "expired", "expirationDate")}}
        for relation in ("buildBetaDetail", "preReleaseVersion"):
            target = item.get("relationships", {}).get(relation, {}).get("data")
            result[relation] = included.get((target["type"], target["id"])) if target else None
        groups = item.get("relationships", {}).get("betaGroups", {})
        result["betaGroups"] = [
            {"id": target["id"], **included.get((target["type"], target["id"]), {})}
            for target in groups.get("data", [])
        ] if "data" in groups else None
        result["betaGroupsHasMore"] = bool(groups.get("links", {}).get("next"))
        builds.append(result)
    return {"platform": platform, "appID": APP_IDS[platform], "builds": builds}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("platform", choices=APP_IDS)
    parser.add_argument("action", choices=["status"])
    args = parser.parse_args()
    try:
        print(json.dumps(build_status(args.platform), ensure_ascii=False, indent=2))
    except (ValueError, RuntimeError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
