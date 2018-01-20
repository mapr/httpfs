package org.apache.hadoop.fs.http.server;

import javax.servlet.Filter;
import javax.servlet.FilterConfig;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;

/**
 * Filter which adds the configured header to every request
 */
public class RequestContentTypeFilter implements Filter {
    private String contentType = "application/octet-stream";



    public void init(FilterConfig filterConfig) {
    }

    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest request = new EnsureContentTypePresent((HttpServletRequest) req, contentType);
        chain.doFilter(request, res);
    }

    public void destroy() {
        this.contentType = null;
    }

    private class EnsureContentTypePresent extends HttpServletRequestWrapper {
        private String contentType;

        public EnsureContentTypePresent(HttpServletRequest request, String contentType) {
            super(request);
            this.contentType = contentType;
        }

        @Override
        public String getContentType() {
            String method = this.getMethod();

            if (method.equals("PUT") || method.equals("POST")) {
                return contentType;
            }
            return super.getContentType();
        }
    }
}