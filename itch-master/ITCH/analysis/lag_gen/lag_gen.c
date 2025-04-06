#include <stdio.h>

int cmin(int n1, int n2) {
    if (n1 <= n2) 
        return n1;
    else
        return n2;
}

void create_lags(double *array_in,int *array_out, int n, double lag_length) {
    // correct      {2,2,3,4,5,5,6,8,
    double threshold;
    int current_place;
    int ii = 0;
    if (n < 2)
        return;

    for (ii = 0; ii < n-1; ii++) {
        threshold = array_in[ii] + lag_length;
        current_place = ii + 1;
        while (threshold > array_in[current_place]) {
            current_place += 1;
            if (current_place >= n)
                break;
        }
        // This appears to already deal with the end ones by assigning the last column.  Check that.
        // Make the last one be missing
        array_out[ii] = current_place-1;
        // The last one should be missing so we set it to a negative number
    }
    return;
}




/*
int main() {
    int n = 10;
    double lag = .003;
    double times[10] = {.001,.002,.003,.005,.006,.008,.011,.014,.015,.017};
    // correct      {2,2,3,4,5,5,6,8,9,-1}
    int lag_times[10];
    create_lags(&times[0],&lag_times[0],n,lag);
    printf("%d \n",min(3,4));
    printf("%d \n",min(47,8));
    int threshold, current_place;
    for (int ii = 0; ii < n; ii++) {
        threshold = times[ii] + lag;
        current_place = ii + 1;
        while (threshold > times[current_place]) {
            current_place += 1;
        }
        // This appears to already deal with the end ones by assigning the last column.  Check that.
        lag_times[ii] = current_place-1;
    }
    for (int ii = 0; ii < n; ii++) {
        printf("%d ", lag_times[ii]);
    }
    printf("\n");
    return 0;
}
*/
