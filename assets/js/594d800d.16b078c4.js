"use strict";(self.webpackChunkreplayforge=self.webpackChunkreplayforge||[]).push([[1677],{9570:(n,e,s)=>{s.r(e),s.d(e,{assets:()=>l,contentTitle:()=>t,default:()=>p,frontMatter:()=>a,metadata:()=>o,toc:()=>c});var i=s(4848),r=s(8453);const a={sidebar_position:1},t="Use Var envs in configuration files",o={id:"advanced/var-envs",title:"Use Var envs in configuration files",description:"JSON Configuration with Environment Variables",source:"@site/docs/advanced/var-envs.md",sourceDirName:"advanced",slug:"/advanced/var-envs",permalink:"/replayforge/docs/advanced/var-envs",draft:!1,unlisted:!1,editUrl:"https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/docs/advanced/var-envs.md",tags:[],version:"current",sidebarPosition:1,frontMatter:{sidebar_position:1},sidebar:"tutorialSidebar",previous:{title:"Use Tailscale",permalink:"/replayforge/docs/advanced/tailscale"},next:{title:"Log level",permalink:"/replayforge/docs/advanced/log-level"}},l={},c=[{value:"JSON Configuration with Environment Variables",id:"json-configuration-with-environment-variables",level:2},{value:"Basic Usage:",id:"basic-usage",level:3},{value:"Special Features:",id:"special-features",level:3},{value:"Examples:",id:"examples",level:3},{value:"Notes:",id:"notes",level:3}];function d(n){const e={code:"code",h1:"h1",h2:"h2",h3:"h3",header:"header",li:"li",p:"p",pre:"pre",strong:"strong",ul:"ul",...(0,r.R)(),...n.components};return(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(e.header,{children:(0,i.jsx)(e.h1,{id:"use-var-envs-in-configuration-files",children:"Use Var envs in configuration files"})}),"\n",(0,i.jsx)(e.h2,{id:"json-configuration-with-environment-variables",children:"JSON Configuration with Environment Variables"}),"\n",(0,i.jsxs)(e.p,{children:[(0,i.jsx)(e.strong,{children:"Purpose"}),": Allows you to write JSON configuration files with environment variable support."]}),"\n",(0,i.jsx)(e.h3,{id:"basic-usage",children:"Basic Usage:"}),"\n",(0,i.jsx)(e.pre,{children:(0,i.jsx)(e.code,{className:"language-json",children:'{\n  "database": {\n    "host": "$DB_HOST",\n    "port": "$DB_PORT",\n    "password": "$DB_PASSWORD"\n  },\n  "api": {\n    "url": "https://$API_DOMAIN",\n    "key": "$API_KEY"\n  }\n}\n'})}),"\n",(0,i.jsx)(e.h3,{id:"special-features",children:"Special Features:"}),"\n",(0,i.jsx)(e.p,{children:(0,i.jsx)(e.strong,{children:"1. Environment Variables"})}),"\n",(0,i.jsxs)(e.ul,{children:["\n",(0,i.jsxs)(e.li,{children:["Use ",(0,i.jsx)(e.code,{children:"$VARIABLE_NAME"})," to reference environment variables"]}),"\n",(0,i.jsxs)(e.li,{children:["Example: ",(0,i.jsx)(e.code,{children:"$DB_HOST"})," will be replaced with actual database host"]}),"\n"]}),"\n",(0,i.jsx)(e.p,{children:(0,i.jsx)(e.strong,{children:"2. Escaping Dollar Signs"})}),"\n",(0,i.jsxs)(e.ul,{children:["\n",(0,i.jsxs)(e.li,{children:["Use ",(0,i.jsx)(e.code,{children:"\\$"})," to write literal dollar signs"]}),"\n",(0,i.jsxs)(e.li,{children:["Example: ",(0,i.jsx)(e.code,{children:'"price": "\\$9.99"'})," stays as ",(0,i.jsx)(e.code,{children:'"price": "$9.99"'})]}),"\n"]}),"\n",(0,i.jsx)(e.h3,{id:"examples",children:"Examples:"}),"\n",(0,i.jsx)(e.p,{children:"proxy.json:"}),"\n",(0,i.jsx)(e.pre,{children:(0,i.jsx)(e.code,{className:"language-json",children:'{\n  "sources": [\n    {\n      "id": "appli1",\n      "type": "pgcall",\n      "params": {\n        "intervalSeconds": 60,\n        "host": "$PGM_DB_HOST_APPLI1_API",\n        "port": "$PGM_DB_PORT_APPLI1_API",\n        "database": "$PGM_DB_DATABASE_APPLI1_API",\n        "user": "$PGM_DB_USER_APPLI1_API",\n        "password": "$PGM_DB_PASSWORD_APPLI1_API",\n        "calls": [\n          {\n            "name": "appli1_ping",\n            "sql": "SELECT monitoring.ping()"\n          },\n          {\n            "name": "appli1_connections",\n            "sql": "SELECT monitoring.connections_since_5_minutes()"\n          }\n        ]\n      },\n      "targetSink": "monitoring_sink",\n      "hookInterval": 60000\n    }\n  ],\n  "sinks": [\n    {\n      "id": "monitoring_sink",\n      "type": "http",\n      "url": "http://relay-forwarder:8100/",\n      "buckets": ["monitoring"],\n      "authBearer": "secret",\n      "useTsnet": true\n    }\n  ],\n  "portStatusZ": 8000,\n  "tsnetHostname": "monitoring-proxy",\n  "envName": "monitoring"\n}\n'})}),"\n",(0,i.jsx)(e.h3,{id:"notes",children:"Notes:"}),"\n",(0,i.jsxs)(e.ul,{children:["\n",(0,i.jsx)(e.li,{children:"Environment variables must be set before parsing"}),"\n",(0,i.jsx)(e.li,{children:"Missing environment variables will be replaced with empty strings"}),"\n",(0,i.jsx)(e.li,{children:"Invalid JSON will result in error"}),"\n",(0,i.jsxs)(e.li,{children:[(0,i.jsx)(e.code,{children:"__ESCAPED_DOLLAR__"})," is a reserved string, don't use it in your config"]}),"\n"]})]})}function p(n={}){const{wrapper:e}={...(0,r.R)(),...n.components};return e?(0,i.jsx)(e,{...n,children:(0,i.jsx)(d,{...n})}):d(n)}},8453:(n,e,s)=>{s.d(e,{R:()=>t,x:()=>o});var i=s(6540);const r={},a=i.createContext(r);function t(n){const e=i.useContext(a);return i.useMemo((function(){return"function"==typeof n?n(e):{...e,...n}}),[e,n])}function o(n){let e;return e=n.disableParentContext?"function"==typeof n.components?n.components(r):n.components||r:t(n.components),i.createElement(a.Provider,{value:e},n.children)}}}]);