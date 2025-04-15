import asyncio
import pprint
import json
from typing import Optional

import boto3
import streamlit as st
import pandas as pd

from mcp_client import MultiMCPClient


async def get_table_info(mcp_client, bedrock_client, tools, query_request: str) -> Optional[str]:
    """테이블 구조 정보를 조회하는 함수"""
    system_prompt = """PostgreSQL 데이터베이스의 테이블 정보를 조회합니다.
다음 SQL 쿼리를 사용하여 테이블 정보를 조회해주세요:

SELECT
    t.table_name,
    c.column_name,
    c.data_type,
    c.is_nullable
FROM
    information_schema.tables t
    JOIN information_schema.columns c ON t.table_name = c.table_name
WHERE
    t.table_schema = 'public'
ORDER BY
    t.table_name,
    c.ordinal_position;

결과는 반드시 JSON 배열 형태로 반환해주세요."""

    message_list = [{
        "role": "user",
        "content": [
            {"text": f"다음 요청과 관련된 테이블 정보를 조회해주세요: {query_request}"}
        ],
    }]

    table_info = ""
    with st.spinner('테이블 구조 정보를 조회중입니다...'):
        while True:
            response = bedrock_client.converse(
                modelId="us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                messages=message_list,
                system=[{"text": system_prompt}],
                toolConfig={
                    "tools": tools
                },
            )

            if response['stopReason'] != 'tool_use':
                break

            tool_requests = response['output']['message']['content']
            message_list.append(response['output']['message'])

            for tool_request in tool_requests:
                if 'toolUse' in tool_request:
                    tool = tool_request['toolUse']
                    tool_id = tool['toolUseId']
                    tool_name = tool['name']
                    tool_input = tool['input']

                    tool_result = await mcp_client.call_tool(tool_name, tool_input)
                    table_info = tool_result.content[0].text

                    message_list.append({
                        "role": "user",
                        "content": [{
                            "toolResult": {
                                "toolUseId": tool_id,
                                "content": [{"text": table_info}]
                            }
                        }],
                    })

    return table_info

def format_sql_result(result_text: str):
    """SQL 쿼리 결과를 테이블 형태로 포맷팅"""
    try:
        # 결과가 JSON 형태인 경우
        data = json.loads(result_text)
        if isinstance(data, list) and len(data) > 0:
            df = pd.DataFrame(data)
            return df
    except:
        # JSON 파싱 실패시 원본 텍스트 반환
        return result_text

async def process_query(prompt: str, bedrock_client, mcp_client, tools, system_prompt: str):
    """사용자 쿼리를 처리하는 함수"""
    if not prompt:
        return

    message_list = [{
        "role": "user",
        "content": [
            {"text": prompt}
        ],
    }]

    while True:
        with st.spinner('Agent가 응답을 생성중입니다...'):
            response = bedrock_client.converse(
                modelId="us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                messages=message_list,
                system=[{"text": system_prompt}],
                toolConfig={
                    "tools": tools
                },
            )

        with st.expander("Agent의 응답", expanded=True):
            # text 부분만 추출하여 표시
            for content in response['output']['message']['content']:
                if 'text' in content:
                    st.markdown("""---""")
                    st.markdown(content['text'])

        if response['stopReason'] != 'tool_use':
            break

        tool_requests = response['output']['message']['content']
        message_list.append(response['output']['message'])

        for tool_request in tool_requests:
            if 'toolUse' in tool_request:
                tool = tool_request['toolUse']
                tool_id = tool['toolUseId']
                tool_name = tool['name']
                tool_input = tool['input']

                with st.expander("실행할 쿼리", expanded=True):
                    st.markdown("""---""")
                    st.markdown("### 실행 쿼리")
                    st.code(tool_input['sql'], language='sql')

                with st.spinner('SQL 쿼리를 실행중입니다...'):
                    tool_result = await mcp_client.call_tool(tool_name, tool_input)

                with st.expander("쿼리 실행 결과", expanded=True):
                    st.markdown("""---""")
                    st.markdown("### 실행 결과")
                    result = format_sql_result(tool_result.content[0].text)
                    if isinstance(result, pd.DataFrame):
                        st.dataframe(
                            result,
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.write(result)

                message_list.append({
                    "role": "user",
                    "content": [{
                        "toolResult": {
                            "toolUseId": tool_id,
                            "content": [{"text": tool_result.content[0].text}]
                        }
                    }],
                })

async def main():
    st.set_page_config(
        page_title="SQL Query Assistant",
        page_icon="🔍",
        layout="wide"
    )

    st.title("SQL Query Assistant")
    st.write("테이블 구조를 기반으로 SQL 쿼리를 생성하고 실행합니다.")

    # 세션 상태 초기화
    if 'is_running' not in st.session_state:
        st.session_state.is_running = False
    if 'should_cancel' not in st.session_state:
        st.session_state.should_cancel = False

    bedrock_client = boto3.client(
        service_name="bedrock-runtime"
    )

    MCP_SERVERS_CONFIG = {
        "postgres": {
            "command": "npx",
            "args": [
                "-y",
                "@modelcontextprotocol/server-postgres",
                "postgresql://postgres:postgres@localhost:5432/sqlquery"
            ]
        }
    }

    # 쿼리 입력 받기
    query = st.text_area(
        "SQL 쿼리를 자연어로 입력하세요",
        height=100,
        placeholder="예: 부서별 모니터 신청 현황을 조회해주세요"
    )

    # 버튼 컨테이너 생성
    col1, col2 = st.columns([1, 6])
    with col1:
        if st.button("쿼리 실행", type="primary", disabled=st.session_state.is_running):
            st.session_state.is_running = True
            st.session_state.should_cancel = False
            st.rerun()
    with col2:
        if st.session_state.is_running:
            if st.button("취소", type="secondary"):
                st.session_state.should_cancel = True
                st.session_state.is_running = False
                st.rerun()

    if st.session_state.is_running and query:
        async with MultiMCPClient(MCP_SERVERS_CONFIG) as mcp_client:
            tools = await mcp_client.list_all_tools()
            print(tools)
            
            # 취소 확인
            if st.session_state.should_cancel:
                st.warning("쿼리 실행이 취소되었습니다.")
                st.session_state.is_running = False
                st.rerun()
                return

            # 1. 입력된 쿼리와 관련된 테이블 정보 조회
            table_info = await get_table_info(mcp_client, bedrock_client, tools, query)

            # 취소 확인
            if st.session_state.should_cancel:
                st.warning("쿼리 실행이 취소되었습니다.")
                st.session_state.is_running = False
                st.rerun()
                return

            # 테이블 구조 표시
            with st.expander("관련 테이블 구조 정보", expanded=False):
                st.markdown("""---""")
                st.markdown("### 테이블 구조")
                st.code(table_info, language='sql')

            # 2. 시스템 프롬프트 생성
            system_prompt = f"""다음은 요청하신 쿼리와 관련된 테이블 구조입니다:

{table_info}

위 테이블 구조를 참고하여 사용자의 요청에 맞는 SQL SELECT 쿼리를 생성해주세요.
생성된 쿼리는 실제 실행 가능해야 합니다.
WHERE 절이나 조건문에는 적절한 임의의 값을 사용해주세요. (예: id = '123', date = '2024-03-21' 등)
CREATE나 INSERT 문은 생성하지 말고, SELECT 문만 작성해주세요.
결과는 반드시 JSON 배열 형태로 반환해주세요."""

            # 3. 쿼리 처리 및 실행
            await process_query(
                query,
                bedrock_client,
                mcp_client,
                tools,
                system_prompt
            )

            # 실행 완료 후 상태 초기화
            st.session_state.is_running = False
            st.rerun()

if __name__ == "__main__":
    asyncio.run(main())