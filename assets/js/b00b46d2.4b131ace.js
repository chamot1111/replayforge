"use strict";(self.webpackChunkreplayforge=self.webpackChunkreplayforge||[]).push([[4563],{981:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>i,contentTitle:()=>a,default:()=>p,frontMatter:()=>o,metadata:()=>c,toc:()=>d});var r=t(4848),s=t(8453);const o={sidebar_position:1},a="Transform Scripts",c={id:"tutorial-transform-scripts/transform-scripts",title:"Transform Scripts",description:"Script structure",source:"@site/docs/tutorial-transform-scripts/transform-scripts.md",sourceDirName:"tutorial-transform-scripts",slug:"/tutorial-transform-scripts/transform-scripts",permalink:"/replayforge/docs/tutorial-transform-scripts/transform-scripts",draft:!1,unlisted:!1,editUrl:"https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/docs/tutorial-transform-scripts/transform-scripts.md",tags:[],version:"current",sidebarPosition:1,frontMatter:{sidebar_position:1},sidebar:"tutorialSidebar",previous:{title:"Tutorial - Transform scripts",permalink:"/replayforge/docs/category/tutorial---transform-scripts"},next:{title:"Use cases",permalink:"/replayforge/docs/category/use-cases"}},i={},d=[{value:"Script structure",id:"script-structure",level:2},{value:"unwrap and wrap content",id:"unwrap-and-wrap-content",level:2},{value:"alert on no message",id:"alert-on-no-message",level:2},{value:"grok parser",id:"grok-parser",level:2}];function l(e){const n={code:"code",h1:"h1",h2:"h2",header:"header",p:"p",pre:"pre",...(0,s.R)(),...e.components};return(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(n.header,{children:(0,r.jsx)(n.h1,{id:"transform-scripts",children:"Transform Scripts"})}),"\n",(0,r.jsx)(n.h2,{id:"script-structure",children:"Script structure"}),"\n",(0,r.jsxs)(n.p,{children:["A transform script is a Lua script that is executed on the player and proxy. It can be used to transform the content of the content before they are sent to the sink.\nThe lua script path is defined in the source configuration by the property ",(0,r.jsx)(n.code,{children:"transformScript"}),"."]}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{className:"language-lua",children:"function Process(content, emit)\n    emit('the-sink', content)\nend\n"})}),"\n",(0,r.jsxs)(n.p,{children:["On player and proxy TimerHandler is called every ",(0,r.jsx)(n.code,{children:"hookInterval"})," duration (define on the source)."]}),"\n",(0,r.jsxs)(n.p,{children:["The ",(0,r.jsx)(n.code,{children:"hookInterval"})," in source configuration supports following duration format: ",(0,r.jsx)(n.code,{children:"300ms"}),", ",(0,r.jsx)(n.code,{children:"-1.5h"})," or ",(0,r.jsx)(n.code,{children:"2h45m"}),"\nValid time units are ",(0,r.jsx)(n.code,{children:"ns"}),", ",(0,r.jsx)(n.code,{children:"us"})," (or ",(0,r.jsx)(n.code,{children:"\xb5s"}),"), ",(0,r.jsx)(n.code,{children:"ms"}),", ",(0,r.jsx)(n.code,{children:"s"}),", ",(0,r.jsx)(n.code,{children:"m"}),", ",(0,r.jsx)(n.code,{children:"h"}),"."]}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{className:"language-lua",children:"function TimerHandler(emit)\n    emit('the-sink', json_encode({\n        body = 'Hello, world!',\n        method = 'POST',\n    }))\nend\n"})}),"\n",(0,r.jsx)(n.h2,{id:"unwrap-and-wrap-content",children:"unwrap and wrap content"}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{className:"language-lua",children:"\nfunction Process(content, emit)\n    local data = json_decode(content)\n    local body = data['body']\n\n    -- body can be plain text or json\n    -- if it's json, we can access its fields using json_decode(body)\n\n    local ip = data['ip']\n    local path = data['path']\n    local params = data['params']\n    local headers = data['headers']\n\n    local wrapped = json_encode({\n        body = body,\n        ip = ip,\n        path = path,\n        params = params,\n        headers = headers,\n        method = r.Method,\n    })\n\n    emit('the-sink', wrapped)\nend\n"})}),"\n",(0,r.jsx)(n.h2,{id:"alert-on-no-message",children:"alert on no message"}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{className:"language-lua",children:"local last_message_time = os.time()\nlocal alert_sent = false\n\nfunction TimerHandler(emit)\n    local current_time = os.time()\n    if current_time - last_message_time > 300 and not alert_sent then -- 5 minutes = 300 seconds\n        emit('notifications', json_encode({\n            method = 'POST',\n            body = json_encode({\n                message = 'No logs received in the last 5 minutes',\n            })\n        }))\n        alert_sent = true\n    end\nend\n\nfunction Process(content, emit)\n    last_message_time = os.time()\n    if alert_sent then\n        emit('notifications', json_encode({\n            body = json_encode({\n                message = 'Service restored - logs are being received again',\n            })\n        }))\n        alert_sent = false\n    end\n    emit('the-sink', content)\nend\n"})}),"\n",(0,r.jsx)(n.h2,{id:"grok-parser",children:"grok parser"}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{className:"language-lua",children:'env_name = os.getenv("ENV_NAME")\ncount = 0\n\nfunction Process(content, emit)\n    contentMap = json_decode(content)\n    bodyStr = contentMap["body"]\n\n    body = json_decode(bodyStr)\n    content = body["content"]\n\n    gr = "%{IPORHOST:client_ip} %{USER:ident} %{USER:auth} \\\\[%{HTTPDATE:timestamp}\\\\] \\"%{WORD:http_method} %{NOTSPACE:request_path} HTTP/%{NUMBER:http_version}\\" %{NUMBER:response_code:int} %{NUMBER:bytes:int} \\"%{DATA:referer}\\" \\"%{DATA:user_agent}\\""\n    values = grok_parse(gr, content)\n\n    count = count + 1\n\n    if values and (count % 100 == 0 or (values.response_code and tonumber(values.response_code) > 399)) then\n        if count % 100 == 0 then\n            count = 0\n        end\n        values.env_name = env_name\n        contentMap["body"] = json_encode(values)\n        emit("nginx_log_sink", json_encode(contentMap))\n    end\nend\n'})})]})}function p(e={}){const{wrapper:n}={...(0,s.R)(),...e.components};return n?(0,r.jsx)(n,{...e,children:(0,r.jsx)(l,{...e})}):l(e)}},8453:(e,n,t)=>{t.d(n,{R:()=>a,x:()=>c});var r=t(6540);const s={},o=r.createContext(s);function a(e){const n=r.useContext(o);return r.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function c(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(s):e.components||s:a(e.components),r.createElement(o.Provider,{value:n},e.children)}}}]);