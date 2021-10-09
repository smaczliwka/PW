package cp1.solution;

import cp1.base.ResourceOperation;
import cp1.base.Resource;

public class MyPair {
    private ResourceOperation op;
    private Resource res;

    MyPair(ResourceOperation op, Resource res) {
        this.op = op;
        this.res = res;
    }

    public Resource res() {
        return res;
    }
    public ResourceOperation op() {
        return op;
    }
}
