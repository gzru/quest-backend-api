
local function access_filter(user_id)
    return function(rec)
        local rec_is_private = rec['is_private']
        if rec_is_private == 0 then
            return true
        end
        local rec_user_id = rec['user_id']
        if rec_user_id == user_id then
            return true
        end
        return false
    end
end

local function format_result(rec)
    return map { sign_id = rec['sign_id'], location = rec['location'] }
end

local function ranker(features)
    local features_size = list.size(features)
    return function(rec)
        local result = map { sign_id = rec['sign_id'], location = rec['location'] }
        local r_features = rec['features']
        if not r_features or list.size(r_features) ~= features_size then
            return result
        end
        local rank = 0
        for i = 1, features_size do
            rank = rank + features[i] * r_features[i]
        end
        result['rank'] = rank
        return result
    end
end

function apply_access_filter(stream, user_id)
    return stream : filter(access_filter(user_id)) : map(format_result)
end

function apply_ranking(stream, user_id, features)
    return stream : filter(access_filter(user_id)) : map(ranker(features))
end

