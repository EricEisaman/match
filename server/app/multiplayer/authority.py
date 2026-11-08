"""Authority: item owners plus env authority.

Ownership changes on claim, release, transfer, arrival, and leave.
"""

from typing import Dict, List, Optional


class AuthorityManager:
    def __init__(self):
        self.itemOwners: Dict[str, Dict] = {}
        self.envArrivalOrder: Dict[str, List[str]] = {}
        self.envAuthority: Dict[str, str] = {}
        self.clientEnvs: Dict[str, str] = {}

    def client_arrives(self, client_id: str, env_name: str):
        self.clientEnvs[client_id] = env_name
        if env_name not in self.envArrivalOrder:
            self.envArrivalOrder[env_name] = []
        if client_id not in self.envArrivalOrder[env_name]:
            self.envArrivalOrder[env_name].append(client_id)
        if env_name not in self.envAuthority:
            self.envAuthority[env_name] = client_id
            return {"became_authority": True, "previous": None, "new": client_id, "reason": "arrival"}
        return {"became_authority": False}

    def client_leaves(self, client_id: str):
        env_name = self.clientEnvs.pop(client_id, None)
        if not env_name:
            return []
        events = []
        if env_name in self.envArrivalOrder:
            if client_id in self.envArrivalOrder[env_name]:
                self.envArrivalOrder[env_name].remove(client_id)
        if self.envAuthority.get(env_name) == client_id:
            if self.envArrivalOrder.get(env_name):
                new_auth = self.envArrivalOrder[env_name][0]
                self.envAuthority[env_name] = new_auth
                events.append({
                    "type": "env-authority-changed",
                    "environmentName": env_name,
                    "previousAuthorityId": client_id,
                    "newAuthorityId": new_auth,
                    "reason": "failover",
                })
            else:
                del self.envAuthority[env_name]
                events.append({
                    "type": "env-authority-changed",
                    "environmentName": env_name,
                    "previousAuthorityId": client_id,
                    "newAuthorityId": None,
                    "reason": "disconnect",
                })
        to_release = [iid for iid, data in self.itemOwners.items() if data["ownerClientId"] == client_id]
        for iid in to_release:
            del self.itemOwners[iid]
            events.append({
                "type": "item-authority-changed",
                "instanceId": iid,
                "previousOwnerId": client_id,
                "newOwnerId": None,
                "reason": "disconnect",
            })
        return events

    def claim(self, instanceId: str, client_id: str):
        existing = self.itemOwners.get(instanceId)
        if not existing:
            self.itemOwners[instanceId] = {"ownerClientId": client_id}
            return {"accepted": True, "previous": None, "new": client_id, "reason": "claim"}
        if existing["ownerClientId"] == client_id:
            return {"accepted": True, "previous": client_id, "new": client_id, "reason": "refresh", "already_owned": True}
        return {"accepted": False, "currentOwnerId": existing["ownerClientId"]}

    def release(self, instanceId: str, client_id: str):
        existing = self.itemOwners.get(instanceId)
        if not existing or existing["ownerClientId"] != client_id:
            return {"released": False}
        del self.itemOwners[instanceId]
        return {"released": True, "previous": client_id, "new": None, "reason": "release"}

    def transfer(self, instanceId: str, client_id: str, new_owner_id: str):
        existing = self.itemOwners.get(instanceId)
        if not existing or existing["ownerClientId"] != client_id:
            return {"transferred": False}
        self.itemOwners[instanceId] = {"ownerClientId": new_owner_id}
        return {"transferred": True, "previous": client_id, "new": new_owner_id, "reason": "transfer"}

    def refresh(self, instanceId: str, client_id: str):
        existing = self.itemOwners.get(instanceId)
        return bool(existing and existing["ownerClientId"] == client_id)

    def get_resolved_owner(self, instanceId: str, env_name: str) -> Optional[str]:
        if instanceId in self.itemOwners:
            return self.itemOwners[instanceId]["ownerClientId"]
        return self.envAuthority.get(env_name)

    def get_env_authority(self, env_name: str) -> Optional[str]:
        return self.envAuthority.get(env_name)
